"""Bronze layer: raw ingestion of the TCE-ES empenhos CSVs.

No content transformation happens here. Every column lands as a string,
under its original source name — including ``EsferaAdministrativa``, which
actually carries the municipality name (a source labelling error; see
docs/data-profiling.md). Reconciling the 55- vs 58-column schemas is the
Silver layer's job, not this one.

Each year is written independently, partitioned by municipality and year,
so the two schemas never need to unify here. Parts within a year (3 for
2020-2023, 5 for 2024+) are concatenated, since they are confirmed
horizontal splits of the same table rather than distinct entities.
"""

import argparse
import re
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SOURCE_ENCODING = "windows-1252"
FIELD_DELIMITER = ";"
MUNICIPALITY_COLUMN = "EsferaAdministrativa"
YEAR_PARTITION_COLUMN = "ano_particao"
FILENAME_PATTERN = re.compile(r"municipios-despesas-empenhos-(?P<part>\d)-(?P<year>\d{4})\.csv$")


def build_spark_session(app_name: str = "es-empenhos-bronze") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        # Each year's write must only touch that year's partitions, not
        # wipe the whole Bronze output every time this runs.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def _year_from_filename(filename: str) -> str:
    match = FILENAME_PATTERN.search(filename)
    if not match:
        raise ValueError(f"Unrecognised source file name: {filename}")
    return match.group("year")


def read_source_file(spark: SparkSession, source_path: Path) -> DataFrame:
    """Read one raw CSV part as-is: every column stays a string."""
    return (
        spark.read.option("header", True)
        .option("sep", FIELD_DELIMITER)
        .option("encoding", SOURCE_ENCODING)
        .option("multiLine", True)
        .csv(str(source_path))
    )


def with_provenance(df: DataFrame, source_path: Path, ingested_at: datetime, year: str) -> DataFrame:
    return (
        df.withColumn("_source_file", F.lit(source_path.name))
        .withColumn("_ingested_at", F.lit(ingested_at.isoformat()))
        .withColumn(YEAR_PARTITION_COLUMN, F.lit(year))
    )


def ingest_year(spark: SparkSession, raw_dir: Path, year: str, ingested_at: datetime) -> DataFrame:
    files = sorted(raw_dir.glob(f"municipios-despesas-empenhos-*-{year}.csv"))
    if not files:
        raise FileNotFoundError(f"No source files found for year {year} in {raw_dir}")

    frames = [with_provenance(read_source_file(spark, f), f, ingested_at, year) for f in files]
    year_df = frames[0]
    for frame in frames[1:]:
        year_df = year_df.unionByName(frame)
    return year_df


def ingest(spark: SparkSession, raw_dir: Path, output_dir: Path, years: list[str]) -> None:
    ingested_at = datetime.now(timezone.utc)
    for year in years:
        year_df = ingest_year(spark, raw_dir, year, ingested_at)
        (
            year_df.write.mode("overwrite")
            .partitionBy(MUNICIPALITY_COLUMN, YEAR_PARTITION_COLUMN)
            .parquet(str(output_dir))
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/lake/bronze/empenhos"))
    parser.add_argument("--years", nargs="+", default=["2023", "2024"])
    args = parser.parse_args()

    spark = build_spark_session()
    try:
        ingest(spark, args.raw_dir, args.output_dir, args.years)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
