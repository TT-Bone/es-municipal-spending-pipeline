"""Gold layer: business-ready aggregates answering three named questions.

1. Committed spending per municipality and year, comparable across municipalities.
2. Carry-over (*restos a pagar*) rate: the share of a year's own commitments
   still unpaid at the end of that year.
3. Supplier concentration: the share of spending going to the largest creditors.

The ``_unknown`` provider (malformed source rows with no municipality, see
docs/data-profiling.md) is excluded from every table: an amount that cannot
be attributed to a municipality cannot be compared across municipalities.
"""

import argparse
from decimal import Decimal
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

MUNICIPALITY_COLUMN = "NomeMunicipio"
UNKNOWN_PROVIDER = "_unknown"
TOP_N_CREDITORS = 10
GENERIC_CREDITOR_PREFIX = "INSCRI"  # "INSCRIÇÃO GENÉRICA - OUTROS": payroll and budget buckets

MONEY = DecimalType(18, 2)
RATIO = DecimalType(9, 4)


def build_spark_session(app_name: str = "es-empenhos-gold") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_silver(spark: SparkSession, silver_dir: Path) -> DataFrame:
    return spark.read.parquet(str(silver_dir))


def prepare(df: DataFrame) -> DataFrame:
    """Drop the unattributable provider and expose the reference year as an
    integer, the grain shared by all three Gold tables."""
    return df.filter(F.col(MUNICIPALITY_COLUMN) != UNKNOWN_PROVIDER).withColumn(
        "ano", F.col("AnoReferencia").cast("int")
    )


def _ratio(numerator: str, denominator: str) -> F.Column:
    """Ratio that is null, not a division error or a misleading 0, when the
    denominator is not positive."""
    return F.when(
        F.col(denominator) > 0, F.col(numerator) / F.col(denominator)
    ).cast(RATIO)


def spending_by_municipality_year(df: DataFrame) -> DataFrame:
    """Q1. Each source row is one month of one empenho, so summing over the
    year yields net committed spending (reversals are negative rows).

    Municipalities differ hugely in size and no population data is in scope,
    so comparability is offered through the share of the state total and the
    rank within the year rather than per-capita figures.
    """
    yearly = df.groupBy(MUNICIPALITY_COLUMN, "ano").agg(
        F.sum("Empenhada").cast(MONEY).alias("empenhada"),
        F.sum("Liquidada").cast(MONEY).alias("liquidada"),
        F.sum("Paga").cast(MONEY).alias("paga"),
    )
    by_year = Window.partitionBy("ano")
    return (
        yearly.withColumn("state_total_empenhada", F.sum("empenhada").over(by_year))
        .withColumn("state_share", _ratio("empenhada", "state_total_empenhada"))
        .withColumn("rank_in_year", F.rank().over(by_year.orderBy(F.col("empenhada").desc())))
        .drop("state_total_empenhada")
    )


def carryover_rate_by_municipality_year(df: DataFrame) -> DataFrame:
    """Q2. Restricted to empenhos issued in the reference year itself: the
    ``Paga`` column also carries payments of prior-year empenhos, which would
    understate the rate. The source's own ``RPNPExercicio``/``RPPExercicio``
    columns are not used because they capture only a tiny fraction of what
    is actually left unpaid (see docs/gold-design.md).
    """
    own_year = df.filter(F.col("AnoEmpenho").cast("int") == F.col("ano"))
    return (
        own_year.groupBy(MUNICIPALITY_COLUMN, "ano")
        .agg(
            F.sum("Empenhada").cast(MONEY).alias("empenhada"),
            F.sum("Paga").cast(MONEY).alias("paga"),
        )
        .withColumn("unpaid", (F.col("empenhada") - F.col("paga")).cast(MONEY))
        .withColumn("carryover_rate", _ratio("unpaid", "empenhada"))
    )


def supplier_concentration_by_municipality_year(
    df: DataFrame, top_n: int = TOP_N_CREDITORS
) -> DataFrame:
    """Q3. Share of committed spending going to the ``top_n`` largest creditors.

    Creditors are keyed by their code, falling back to their name because
    individuals (CPF) come without a code. Generic budget buckets are not
    suppliers and are excluded from the ranking; their weight is reported
    separately so the denominator stays honest.
    """
    generic = F.col("NomeIdentificacao").startswith(GENERIC_CREDITOR_PREFIX)
    creditor_key = F.coalesce(
        F.nullif(F.trim(F.col("CodigoCredorFornecedor")), F.lit("")),
        F.col("NomeCredorFornecedor"),
    )
    per_creditor = (
        df.filter(~generic)
        .withColumn("creditor", creditor_key)
        .groupBy(MUNICIPALITY_COLUMN, "ano", "creditor")
        .agg(F.sum("Empenhada").alias("empenhada"))
        .filter(F.col("empenhada") > 0)
    )
    ranked = per_creditor.withColumn(
        "creditor_rank",
        F.row_number().over(
            Window.partitionBy(MUNICIPALITY_COLUMN, "ano").orderBy(
                F.col("empenhada").desc(), F.col("creditor")
            )
        ),
    )
    concentration = ranked.groupBy(MUNICIPALITY_COLUMN, "ano").agg(
        F.count("*").alias("creditor_count"),
        F.sum("empenhada").cast(MONEY).alias("identified_empenhada"),
        F.sum(F.when(F.col("creditor_rank") <= top_n, F.col("empenhada")))
        .cast(MONEY)
        .alias("top_n_empenhada"),
    )
    generic_totals = (
        df.filter(generic)
        .groupBy(MUNICIPALITY_COLUMN, "ano")
        .agg(F.sum("Empenhada").cast(MONEY).alias("generic_empenhada"))
    )
    return (
        concentration.join(generic_totals, [MUNICIPALITY_COLUMN, "ano"], "left")
        .withColumn("generic_empenhada", F.coalesce(F.col("generic_empenhada"), F.lit(Decimal(0)).cast(MONEY)))
        .withColumn("top_n", F.lit(top_n))
        .withColumn("top_n_share", _ratio("top_n_empenhada", "identified_empenhada"))
    )


def build_gold(df: DataFrame) -> dict[str, DataFrame]:
    prepared = prepare(df)
    return {
        "spending_by_municipality_year": spending_by_municipality_year(prepared),
        "carryover_rate_by_municipality_year": carryover_rate_by_municipality_year(prepared),
        "supplier_concentration_by_municipality_year": supplier_concentration_by_municipality_year(prepared),
    }


def run(spark: SparkSession, silver_dir: Path, output_dir: Path) -> None:
    for name, table in build_gold(read_silver(spark, silver_dir)).items():
        table.write.mode("overwrite").parquet(str(output_dir / name))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-dir", type=Path, default=Path("data/lake/silver/empenhos"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/lake/gold"))
    args = parser.parse_args()

    spark = build_spark_session()
    try:
        run(spark, args.silver_dir, args.output_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
