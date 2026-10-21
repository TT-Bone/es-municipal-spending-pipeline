from datetime import datetime, timezone
from pathlib import Path

import pytest
from pyspark.sql import Row, SparkSession

from pyspark.sql.types import StringType, StructField, StructType

from es_empenhos.bronze.ingest import _year_from_filename, with_provenance
from es_empenhos.quality.checks import QualityCheckError, check_bronze_provenance

_PROVENANCE_SCHEMA = StructType([
    StructField("_source_file", StringType(), True),
    StructField("_ingested_at", StringType(), True),
    StructField("ano_particao", StringType(), True),
])


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = SparkSession.builder.appName("bronze-tests").master("local[1]").getOrCreate()
    yield session
    session.stop()


def test_year_from_filename_parses_year():
    assert _year_from_filename("municipios-despesas-empenhos-1-2023.csv") == "2023"
    assert _year_from_filename("municipios-despesas-empenhos-5-2024.csv") == "2024"


def test_year_from_filename_rejects_unrecognised_name():
    with pytest.raises(ValueError, match="Unrecognised"):
        _year_from_filename("some-other-file.csv")


def test_with_provenance_adds_required_columns(spark):
    df = spark.createDataFrame([Row(Value="x")])
    source = Path("municipios-despesas-empenhos-1-2023.csv")
    ingested_at = datetime(2026, 8, 30, tzinfo=timezone.utc)

    result = with_provenance(df, source, ingested_at, "2023").first()

    assert result._source_file == "municipios-despesas-empenhos-1-2023.csv"
    assert result._ingested_at == "2026-08-30T00:00:00+00:00"
    assert result.ano_particao == "2023"


def test_check_bronze_provenance_passes_when_all_columns_present(spark):
    df = spark.createDataFrame(
        [("f.csv", "2026-08-30T00:00:00+00:00", "2023")], schema=_PROVENANCE_SCHEMA
    )
    check_bronze_provenance(df)  # must not raise


def test_check_bronze_provenance_fails_on_null_provenance(spark):
    df = spark.createDataFrame(
        [(None, "2026-08-30T00:00:00+00:00", "2023")], schema=_PROVENANCE_SCHEMA
    )
    with pytest.raises(QualityCheckError, match="_source_file"):
        check_bronze_provenance(df)
