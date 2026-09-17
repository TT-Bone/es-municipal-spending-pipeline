"""Silver layer: reconciles the 55- vs 58-column Bronze schemas, types the
monetary and date columns, and removes the exact-duplicate rows found only
in 2024.

Every decision here traces back to a finding in docs/data-profiling.md:
the 2023/2024 column rename and additions, the malformed rows with no
municipality, and the 2024 year-end duplicate rows.
"""

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

MUNICIPALITY_COLUMN = "EsferaAdministrativa"
MUNICIPALITY_COLUMN_RENAMED = "NomeMunicipio"
YEAR_PARTITION_COLUMN = "ano_particao"
UNKNOWN_PROVIDER = "_unknown"

# 2020-2023 column name -> 2024+ replacement. Bronze is read with
# mergeSchema, so a legacy row carries null in the replacement column and a
# current row carries null in the legacy one; reconcile_schema coalesces
# each pair and drops the legacy name.
LEGACY_TO_CURRENT_COLUMNS = {
    "Ano": "AnoReferencia",
    "Mes": "MesReferencia",
}

MONETARY_COLUMNS = [
    "Empenhada",
    "Liquidada",
    "Paga",
    "RPNPExercicio",
    "RPPExercicio",
    "RPNPTransferido",
    "RPPTransferido",
    "RPNPBloqueado",
    "RPNPLiquidado",
    "RPNPPago",
    "RPPPago",
    "RPNPCancelado",
    "RPPCancelado",
    "RPNPRecebido",
    "RPNPRestabelecido",
    "RPPRecebido",
]

DATE_COLUMNS = ["DataEmissao"]


def build_spark_session(app_name: str = "es-empenhos-silver") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession, bronze_dir: Path) -> DataFrame:
    """Read the full Bronze output. ``mergeSchema`` reconciles the 55- vs
    58-column layouts at the file level, filling the columns introduced in
    2024 with null for older rows; reconcile_schema finishes the job by
    coalescing the renamed reference-year/month columns."""
    return spark.read.option("mergeSchema", "true").parquet(str(bronze_dir))


def reconcile_schema(df: DataFrame) -> DataFrame:
    """Coalesce each legacy/current column pair produced by mergeSchema and
    drop the legacy name."""
    for legacy, current in LEGACY_TO_CURRENT_COLUMNS.items():
        df = df.withColumn(current, F.coalesce(F.col(current), F.col(legacy))).drop(legacy)
    return df


def rename_municipality_column(df: DataFrame) -> DataFrame:
    """``EsferaAdministrativa`` actually carries the municipality name, not
    the administrative sphere its source name implies (see
    docs/data-profiling.md)."""
    return df.withColumnRenamed(MUNICIPALITY_COLUMN, MUNICIPALITY_COLUMN_RENAMED)


def tag_unknown_provider(df: DataFrame) -> DataFrame:
    """Route the malformed, field-shifted rows that carry no municipality
    into an explicit ``_unknown`` partition instead of dropping them."""
    return df.withColumn(
        MUNICIPALITY_COLUMN_RENAMED,
        F.coalesce(F.col(MUNICIPALITY_COLUMN_RENAMED), F.lit(UNKNOWN_PROVIDER)),
    )


def cast_monetary_columns(df: DataFrame, columns: list[str] = MONETARY_COLUMNS) -> DataFrame:
    """Parse Brazilian-format decimals (``5624,69``) into ``DecimalType``."""
    for column in columns:
        df = df.withColumn(
            column,
            F.regexp_replace(F.col(column), ",", ".").cast(DecimalType(18, 2)),
        )
    return df


def parse_date_columns(df: DataFrame, columns: list[str] = DATE_COLUMNS) -> DataFrame:
    """``DataEmissao`` arrives as ``yyyy-MM-dd HH:mm:ss``, frequently empty."""
    for column in columns:
        df = df.withColumn(column, F.to_timestamp(F.col(column), "yyyy-MM-dd HH:mm:ss"))
    return df


def deduplicate(df: DataFrame) -> DataFrame:
    """Drop the byte-for-byte duplicate rows unique to 2024 (see
    docs/data-profiling.md) before they can be double-counted downstream."""
    return df.dropDuplicates()


def transform(df: DataFrame) -> DataFrame:
    """Apply the full Silver transformation to the raw Bronze output."""
    df = reconcile_schema(df)
    df = rename_municipality_column(df)
    df = tag_unknown_provider(df)
    df = cast_monetary_columns(df)
    df = parse_date_columns(df)
    df = deduplicate(df)
    return df


def run(spark: SparkSession, bronze_dir: Path, output_dir: Path) -> None:
    result = transform(read_bronze(spark, bronze_dir))
    (
        result.write.mode("overwrite")
        .partitionBy(MUNICIPALITY_COLUMN_RENAMED, YEAR_PARTITION_COLUMN)
        .parquet(str(output_dir))
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-dir", type=Path, default=Path("data/lake/bronze/empenhos"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/lake/silver/empenhos"))
    args = parser.parse_args()

    spark = build_spark_session()
    try:
        run(spark, args.bronze_dir, args.output_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
