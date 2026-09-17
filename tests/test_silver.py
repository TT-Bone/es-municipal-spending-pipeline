from decimal import Decimal

import pytest
from pyspark.sql import Row, SparkSession

from es_empenhos.silver.transform import (
    cast_monetary_columns,
    deduplicate,
    parse_date_columns,
    reconcile_schema,
    rename_municipality_column,
    tag_unknown_provider,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = SparkSession.builder.appName("silver-tests").master("local[1]").getOrCreate()
    yield session
    session.stop()


def test_reconcile_schema_coalesces_legacy_and_current_columns(spark):
    # mergeSchema fills the column absent from a given year's files with
    # null, mirroring what read_bronze produces for a 2023 row (Ano set,
    # AnoReferencia null) and a 2024 row (the reverse).
    df = spark.createDataFrame(
        [
            Row(Ano="2023", AnoReferencia=None, Mes="6", MesReferencia=None),
            Row(Ano=None, AnoReferencia="2024", Mes=None, MesReferencia="6"),
        ]
    )

    result = reconcile_schema(df).collect()

    assert "Ano" not in reconcile_schema(df).columns
    assert {row.AnoReferencia for row in result} == {"2023", "2024"}
    assert {row.MesReferencia for row in result} == {"6", "6"}


def test_reconcile_schema_handles_single_year_input(spark):
    # A Silver run over only 2023 (or only 2024) Bronze output never gets the
    # other era's column from mergeSchema, so it must not be required.
    legacy_only = spark.createDataFrame([Row(Ano="2023", Mes="6")])
    current_only = spark.createDataFrame([Row(AnoReferencia="2024", MesReferencia="6")])

    assert reconcile_schema(legacy_only).columns == ["AnoReferencia", "MesReferencia"]
    assert reconcile_schema(current_only).columns == ["AnoReferencia", "MesReferencia"]


def test_rename_municipality_column(spark):
    df = spark.createDataFrame([Row(EsferaAdministrativa="Vitória")])

    result = rename_municipality_column(df)

    assert "NomeMunicipio" in result.columns
    assert result.first().NomeMunicipio == "Vitória"


def test_tag_unknown_provider_fills_null_municipality(spark):
    df = spark.createDataFrame(
        [Row(NomeMunicipio="Vitória"), Row(NomeMunicipio=None)]
    )

    result = {row.NomeMunicipio for row in tag_unknown_provider(df).collect()}

    assert result == {"Vitória", "_unknown"}


def test_cast_monetary_columns_parses_brazilian_decimal(spark):
    df = spark.createDataFrame([Row(Empenhada="5624,69")])

    result = cast_monetary_columns(df, columns=["Empenhada"]).first()

    assert result.Empenhada == Decimal("5624.69")


def test_parse_date_columns_handles_empty_string(spark):
    df = spark.createDataFrame(
        [Row(DataEmissao="2024-03-15 00:00:00"), Row(DataEmissao="")]
    )

    result = [row.DataEmissao for row in parse_date_columns(df).collect()]

    assert result[0] is not None
    assert result[1] is None


def test_deduplicate_drops_exact_duplicate_rows(spark):
    df = spark.createDataFrame(
        [Row(NumeroEmpenho="1", MesReferencia="12") for _ in range(3)]
    )

    assert deduplicate(df).count() == 1
