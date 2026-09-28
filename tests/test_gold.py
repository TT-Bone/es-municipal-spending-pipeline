from decimal import Decimal

import pytest
from pyspark.sql import Row, SparkSession

from es_empenhos.gold.aggregate import (
    carryover_rate_by_municipality_year,
    prepare,
    spending_by_municipality_year,
    supplier_concentration_by_municipality_year,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = SparkSession.builder.appName("gold-tests").master("local[1]").getOrCreate()
    yield session
    session.stop()


def _row(municipality, year="2024", empenho_year=None, empenhada="0", liquidada="0", paga="0",
         code="1", name="ACME", identification="CNPJ"):
    return Row(
        NomeMunicipio=municipality,
        AnoReferencia=year,
        AnoEmpenho=empenho_year or year,
        Empenhada=Decimal(empenhada),
        Liquidada=Decimal(liquidada),
        Paga=Decimal(paga),
        CodigoCredorFornecedor=code,
        NomeCredorFornecedor=name,
        NomeIdentificacao=identification,
    )


def test_prepare_drops_unknown_provider(spark):
    df = spark.createDataFrame([_row("Vitória"), _row("_unknown")])

    assert [r.NomeMunicipio for r in prepare(df).collect()] == ["Vitória"]


def test_spending_sums_months_and_ranks_within_year(spark):
    df = spark.createDataFrame(
        [
            _row("A", empenhada="100"),
            _row("A", empenhada="-40"),  # reversal
            _row("B", empenhada="300"),
        ]
    )

    result = {r.NomeMunicipio: r for r in spending_by_municipality_year(prepare(df)).collect()}

    assert result["A"].empenhada == Decimal("60.00")
    assert result["B"].rank_in_year == 1
    assert result["A"].rank_in_year == 2
    assert result["B"].state_share == Decimal("0.8333")


def test_carryover_ignores_payments_of_prior_year_empenhos(spark):
    # Paying an old empenho in 2024 must not offset 2024's own unpaid balance.
    df = spark.createDataFrame(
        [
            _row("A", empenhada="1000", paga="600"),
            _row("A", empenho_year="2023", empenhada="0", paga="5000"),
        ]
    )

    result = carryover_rate_by_municipality_year(prepare(df)).first()

    assert result.unpaid == Decimal("400.00")
    assert result.carryover_rate == Decimal("0.4000")


def test_carryover_rate_is_null_without_commitments(spark):
    df = spark.createDataFrame([_row("A", empenhada="0", paga="10")])

    assert carryover_rate_by_municipality_year(prepare(df)).first().carryover_rate is None


def test_concentration_top_n_share_and_generic_exclusion(spark):
    df = spark.createDataFrame(
        [
            _row("A", empenhada="70", code="1", name="BIG"),
            _row("A", empenhada="20", code="2", name="MID"),
            _row("A", empenhada="10", code=None, name="PERSON", identification="CPF"),
            _row("A", empenhada="900", code="9", name="PAYROLL",
                 identification="INSCRIÇÃO GENÉRICA - OUTROS"),
        ]
    )

    result = supplier_concentration_by_municipality_year(prepare(df), top_n=1).first()

    assert result.creditor_count == 3  # the CPF creditor is keyed by name
    assert result.top_n_share == Decimal("0.7000")  # 70 / 100, payroll excluded
    assert result.generic_empenhada == Decimal("900.00")
