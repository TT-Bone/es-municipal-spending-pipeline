"""Data quality checks, one per pipeline layer.

Each check raises QualityCheckError when the assertion fails.
Checks are called inside each layer's run() function and are therefore
active whether the pipeline runs from the CLI or via Airflow.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class QualityCheckError(Exception):
    pass


def check_bronze_provenance(df: DataFrame) -> None:
    """Every row must carry the three provenance columns written by ingest.

    Column-presence is checked first (no Spark action); null counts are
    checked second and do trigger a scan — acceptable on Bronze because
    the layer processes one year at a time.
    """
    provenance_cols = ("_source_file", "_ingested_at", "ano_particao")
    missing = [c for c in provenance_cols if c not in df.columns]
    if missing:
        raise QualityCheckError(f"Bronze: provenance columns absent: {missing}")
    for col in provenance_cols:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            raise QualityCheckError(
                f"Bronze: {null_count} rows have null {col!r} — provenance is incomplete"
            )


def check_silver_municipality_never_null(df: DataFrame) -> None:
    """NomeMunicipio must be non-null after tag_unknown_provider.

    The transform coalesces null municipalities to '_unknown'; a null here
    means that step broke.
    """
    null_count = df.filter(F.col("NomeMunicipio").isNull()).count()
    if null_count > 0:
        raise QualityCheckError(
            f"Silver: {null_count} rows have null NomeMunicipio after municipality tagging"
        )


def check_silver_monetary_parse_rate(
    df: DataFrame, column: str = "Empenhada", max_null_rate: float = 0.05
) -> None:
    """Null rate of the primary monetary column must stay below max_null_rate.

    Brazilian-decimal casting (replacing ',' with '.') turns unparseable
    strings into nulls silently. A rate spike signals that the source
    changed its decimal format.
    """
    total = df.count()
    if total == 0:
        return
    null_count = df.filter(F.col(column).isNull()).count()
    rate = null_count / total
    if rate > max_null_rate:
        raise QualityCheckError(
            f"Silver: {rate:.1%} of {column!r} is null after decimal cast "
            f"(threshold {max_null_rate:.1%}) — source format may have changed"
        )


def check_gold_state_share_sums_to_one(df: DataFrame, tolerance: float = 0.01) -> None:
    """state_share in the spending table must sum to 1.0 per year.

    state_share is defined as each municipality's empenhada divided by the
    year total; the sum across all municipalities must equal 1.0.  A
    deviation larger than tolerance means a municipality was silently
    excluded or double-counted in the aggregation.
    """
    bad = (
        df.groupBy("ano")
        .agg(F.sum("state_share").alias("total"))
        .filter(F.abs(F.col("total") - 1.0) > tolerance)
        .collect()
    )
    if bad:
        raise QualityCheckError(
            f"Gold spending: state_share does not sum to 1.0 for "
            f"year(s) {[r.ano for r in bad]} (tolerance ±{tolerance})"
        )
