"""Airflow DAG: end-to-end Bronze → Silver → Gold pipeline for TCE-ES empenhos.

Targets Airflow 2.9+.  The pipeline runs as three sequential BashOperator
tasks, each calling the corresponding layer's CLI entry point.  Quality
checks are embedded inside each layer's run() function and run
automatically as part of the task.

Paths are read from Airflow Variables so the same DAG works in both the
local Docker Compose environment and any other deployment without code
changes.  Each variable falls back to the default local path when absent.

To trigger manually from the CLI:
    airflow dags trigger es_empenhos_pipeline

To override paths for a single run:
    airflow dags trigger es_empenhos_pipeline \\
        --conf '{"years": "2023 2024"}'
"""

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

_raw_dir = Variable.get("es_empenhos_raw_dir", default_var="data/raw")
_bronze_dir = Variable.get("es_empenhos_bronze_dir", default_var="data/lake/bronze/empenhos")
_silver_dir = Variable.get("es_empenhos_silver_dir", default_var="data/lake/silver/empenhos")
_gold_dir = Variable.get("es_empenhos_gold_dir", default_var="data/lake/gold")
_years = Variable.get("es_empenhos_years", default_var="2023 2024")

# Spark binds to the loopback interface under WSL2 without this.
_spark_env = {"SPARK_LOCAL_IP": "127.0.0.1"}

with DAG(
    dag_id="es_empenhos_pipeline",
    description="Bronze → Silver → Gold ingestion of TCE-ES municipal spending data.",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # manual trigger only; batch is not meaningfully periodic
    catchup=False,
    tags=["es-empenhos"],
    doc_md=__doc__,
) as dag:

    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=(
            f"python -m es_empenhos.bronze.ingest "
            f"--raw-dir {_raw_dir} "
            f"--output-dir {_bronze_dir} "
            f"--years {_years}"
        ),
        env=_spark_env,
        append_env=True,
        doc_md=(
            "Reads the raw TCE-ES CSVs from *raw_dir*, concatenates the "
            "year parts, adds provenance columns, and writes partitioned "
            "Parquet to *bronze_dir*.  Runs the Bronze provenance quality "
            "check before writing."
        ),
    )

    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command=(
            f"python -m es_empenhos.silver.transform "
            f"--bronze-dir {_bronze_dir} "
            f"--output-dir {_silver_dir}"
        ),
        env=_spark_env,
        append_env=True,
        doc_md=(
            "Reads Bronze Parquet, reconciles the 2023 (55-column) and "
            "2024+ (58-column) schemas, types monetary and date columns, "
            "deduplicates, and writes Silver Parquet.  Runs municipality-"
            "null and monetary-parse-rate quality checks before writing."
        ),
    )

    gold_aggregate = BashOperator(
        task_id="gold_aggregate",
        bash_command=(
            f"python -m es_empenhos.gold.aggregate "
            f"--silver-dir {_silver_dir} "
            f"--output-dir {_gold_dir}"
        ),
        env=_spark_env,
        append_env=True,
        doc_md=(
            "Reads Silver Parquet and writes three Gold tables: "
            "spending_by_municipality_year, carryover_rate_by_municipality_year, "
            "and supplier_concentration_by_municipality_year.  Runs the "
            "state-share integrity check before writing."
        ),
    )

    bronze_ingest >> silver_transform >> gold_aggregate
