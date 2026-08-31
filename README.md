# ES Municipal Spending Pipeline

An end-to-end batch data pipeline over **municipal budget commitments
(*empenhos*) of Espírito Santo, Brazil** — public fiscal-audit data published by
the state court of accounts (TCE-ES).

> **This is a personal portfolio project**, built on open public data. It has not
> been deployed or operated in a production environment.

## Why this dataset

The 78 municipalities reporting to TCE-ES are **independent data providers**.
They share a submission format but not a schema discipline, a release cadence, or
a data steward. That makes this a natural fit for **tenancy-per-provider
ingestion**: each municipality is given isolated custody from the moment the data
lands, before any conforming happens.

The source also evolves without notice. In 2024 the file layout gained three
columns and renamed two, mid-series. Absorbing that break without disturbing
downstream consumers is the central design problem this pipeline solves, and it
is documented rather than papered over.

See [docs/data-profiling.md](docs/data-profiling.md) for the full profile of the
source data.

## Architecture

Medallion layering, with provider isolation established before Bronze.

| Layer | Responsibility | Status |
|---|---|---|
| Bronze | Raw ingestion, no content transformation. Partitioned by provider (municipality) and year. Carries provenance: source file, ingestion timestamp. | Planned |
| Silver | Typing, cleaning, and reconciliation of the 2020–2023 and 2024+ schemas. Brazilian decimals and CP1252 encoding normalised. | Planned |
| Gold | Business-ready aggregates answering named questions (below). | Planned |

An architecture diagram is added once the layers are implemented.

### Questions the Gold layer answers

1. Committed spending per municipality and year, comparable across municipalities.
2. **Carry-over rate (*restos a pagar*) per municipality and year** — how much was
   committed but left unpaid within the fiscal year. A real fiscal-health signal.
3. Supplier concentration per municipality — the share of spending going to the
   largest creditors.

## Stack

| Component | Role | Status |
|---|---|---|
| PySpark | Layer transformations | Planned |
| Apache Airflow | Single end-to-end DAG, Bronze → Silver → Gold | Planned |
| Docker Compose | One-command local stack | Planned |
| Custom PySpark checks | Data quality, one per layer | Planned |

Nothing in this table is claimed as working until it is marked otherwise.

## Environment

Development runs on **WSL2**, on the Linux filesystem rather than through
`/mnt/*`, because Airflow requires POSIX and Spark I/O over the Windows interop
mount is markedly slower.

| | Version |
|---|---|
| OS | Ubuntu 22.04 (WSL2 on Windows 11) |
| Python | 3.10.12 |
| Spark | to be pinned when the Bronze layer lands |
| Java | to be pinned when the Bronze layer lands |

## Data is not in this repository

The source data is roughly 13 GB and is deliberately excluded from version
control. The pipeline processes **2023 and 2024** — one year of each source
schema, which is the minimum that exercises the schema reconciliation. Extending
to further years is a configuration change, since Bronze partitions by year.

To obtain the data, download the *Empenhos Municipais* CSVs for the years you
want from `dados.es.gov.br` and place them in `data/raw/`. The portal has no
active CKAN datastore, so there is no query API to script against.

A small committed sample under `sample/` will let the pipeline run end to end
without the full download.

## Repository layout

```
data/raw/     source CSVs (git-ignored)
data/lake/    Bronze / Silver / Gold outputs (git-ignored)
sample/       small committed extract, enough to run the pipeline
src/          pipeline code, one package per layer
dags/         Airflow DAG
tests/        one test per layer
docs/         source data profiling and design notes
```

## License

Source data is published by TCE-ES as Brazilian open government data.
