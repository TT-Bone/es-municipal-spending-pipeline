# Source Data Profiling

Findings from opening the actual files before writing any transformation code.
Everything below was verified against the downloaded CSVs on **2026-08-31**, not
assumed from the portal documentation.

## Source

| | |
|---|---|
| Publisher | TCE-ES (Tribunal de Contas do Estado do Espírito Santo) |
| Portal | `dados.es.gov.br` |
| Dataset | Empenhos Municipais (municipal budget commitments) |
| Access | Manual download. The CKAN datastore is inactive, so there is no REST/query API. |
| Delivery format | Plain `.csv` files (not archives) |
| Years available | 2020–2026 |
| Files | 28 |
| Total size | ~13 GB uncompressed |

## File layout

Files are named `municipios-despesas-empenhos-{part}-{year}.csv`.

| Years | Parts per year |
|---|---|
| 2020–2023 | 3 |
| 2024–2026 | 5 |

**The parts are horizontal splits of one table, not different entities.** Every
part of a given year carries a byte-identical header, and rows for the same
municipality appear across different parts. The pipeline therefore concatenates
parts within a year rather than joining them.

2026 is a partial year: parts 1 and 2 hold data, parts 3–5 contain only a header.

## Schema evolution: 2023 → 2024

The part count changed at the same time as the schema, but the two are unrelated.
The real break is at the column level.

| | 2020–2023 | 2024–2026 |
|---|---|---|
| Column count | **55** | **58** |
| Reference year | `Ano` | `AnoReferencia` |
| Reference month | `Mes` | `MesReferencia` |
| Accounting year | — | `AnoCompetencia` |
| Issue date | — | `DataEmissao` |
| Funding source detail | — | `CodigoComplementoFontesDestinacoesRecursos` |

Two renames and three additions, introduced without a version marker in the file
name or a notice in the portal metadata. Reconciling these two shapes into one
table is the core job of the Silver layer.

## Parsing rules

| Property | Value |
|---|---|
| Encoding | **CP1252 / Latin-1** — not UTF-8. Confirmed via mojibake on accented names (`Afonso Cláudio`, `ADMINISTRAÇÃO`). |
| Field delimiter | `;` |
| Quote character | `"` (applied to text fields only, inconsistently) |
| Decimal separator | `,` (e.g. `5624,69`) |
| Thousands separator | none observed |
| Date format | `yyyy-MM-dd HH:mm:ss` in `DataEmissao`, frequently empty |

## Provider identity (tenancy key)

- **Confirmed against the full 2023+2024 Bronze output** (not just the sample):
  `EsferaAdministrativa` has 79 distinct values — the 78 official Espírito
  Santo municipalities, spelled consistently with no casing or accent
  variants, plus one `null`. The tenancy-per-provider angle holds: this
  column is a clean municipality key.
- **The `null` value is 1,094 malformed rows, 2023 only.** In each, the row
  has shifted left by one field: `CodigoUnidadeGestora` (first column) holds
  the literal string `"CNPJ"` — the value that belongs in the *last* column,
  `NomeIdentificacao` — and `UnidadeGestora`, `CodigoOrgao`, `NomeOrgao` are
  also `null`. Distribution: part 1 (387 rows), part 2 (485), part 3 (222);
  zero in 2024. Volume is negligible (0.019% of 5,655,253 total rows) and
  immaterial to any aggregate in the Gold layer.
  The Bronze reader already runs with `multiLine=True` (see
  `bronze/ingest.py`), so a plain embedded line break inside a quoted field
  is already handled correctly and is not the cause. The defect surviving
  `multiLine=True` points instead to **unbalanced or malformed quote
  characters** in the source row — consistent with the quoting being
  "applied to text fields only, inconsistently" (see Parsing rules above),
  which can desynchronize the parser's quote state regardless of line
  handling. A companion count of rows with a valid municipality but a `null`
  in the *last* column (the mirror-image symptom of the same shift) came
  back close in order of magnitude (1,165) but not a clean 1:1 pairing, and
  those 1,094 tail rows collapse to only 148 distinct rows under exact
  duplication — an unmapped replication pattern, left uninvestigated since it
  doesn't change the decision below.
  **Decision:** the Bronze keeps these rows as-is (they are evidence of a
  real source defect). The Silver isolates them into an explicitly named
  `_unknown` partition instead of silently dropping them.
- The municipality name is carried in the column named **`EsferaAdministrativa`**.
  This contradicts the official data dictionary, which describes that field as
  "administrative sphere of the managing unit". The observed values are plain
  municipality names (`Aracruz`, `Vitória`). The name is treated as a source
  labelling error and mapped to a correctly named column downstream.
- `CodigoUnidadeGestora` follows the pattern `009E0600001`, whose leading digits
  identify the municipality.
- Below the municipality there are **475 distinct `UnidadeGestora` values** in
  2020 alone — city halls, city councils, municipal funds and pension institutes.
  This gives a second, finer provider level if it is ever needed.

## Empenho lifecycle

The full commitment lifecycle is present, which makes fiscal-health metrics
computable directly from the source:

- `Empenhada` → `Liquidada` → `Paga`
- 13 carry-over (*restos a pagar*) columns: `RPNPExercicio`, `RPPExercicio`,
  `RPNPTransferido`, `RPPTransferido`, `RPNPBloqueado`, `RPNPLiquidado`,
  `RPNPPago`, `RPPPago`, `RPNPCancelado`, `RPPCancelado`, `RPNPRecebido`,
  `RPNPRestabelecido`, `RPPRecebido`

## Row grain

A single `empenho` (identified by `CodigoUnidadeGestora` + `NumeroEmpenho` +
`AnoEmpenho`) recurs across many rows within a year. This is not duplication:
each row is one month of that empenho's execution. Checked against
`022E0500002|00000027|2023` (11 rows, one per month in 2023):

| Mes | Empenhada | Liquidada | Paga |
|---|---|---|---|
| 1 | 15000,00 | 0,00 | 0,00 |
| 2 | 0,00 | 628,44 | 162,07 |
| 3 | 0,00 | 272,52 | 738,89 |
| ... | 0,00 | ... | ... |

The full committed amount lands in the first month the empenho appears;
subsequent months record that month's `Liquidada`/`Paga` installment. So the
natural business key is **`CodigoUnidadeGestora` + `NumeroEmpenho` +
`AnoEmpenho` + `MesReferencia`**, and it is confirmed unique under that
definition across all three 2023 sample parts. Negative amounts also occur
(e.g. `-10212,70`) and are legitimate reversals of a prior month's entry, not
data errors.

## Known data quality issues

- At least one row in the 2020 files has the literal value `4` where a
  municipality name belongs, which points to an unescaped delimiter or line break
  inside a quoted field. Row-shape validation is a real check here, not a
  decorative one.
- Headers repeat once per part file and must be skipped on concatenation.
- **Exact duplicate rows, 2024 only.** Unlike 2023, the business key above is
  *not* unique in 2024: the same key repeats with byte-for-byte identical rows
  (same values in every column, all monetary columns zero, always
  `MesReferencia = 12`). Example: empenho `022E0700001|00000002|2024|12`
  appears **38 times**, all identical, all within the single file
  `municipios-despesas-empenhos-4-2024.csv` (not split across parts — ruled
  out as a part-overlap artifact). 45 such duplicate groups were found in that
  one file alone, more in part 5. Among well-formed rows, 2023 has zero exact
  duplicates across all three parts. The only 2023 repeats are inside the 1,094
  malformed rows described under *Provider identity*, which collapse to 148
  distinct rows. This looks like a year-end close artifact on the source side,
  not a modelling case to preserve.
  **Decision:** the Silver layer drops exact duplicate rows
  (`dropDuplicates()` across all columns) before any aggregation. Since the
  copies carry no new information, this cannot lose real data, and it
  prevents these zero-value reinforcement rows from being double-counted
  if a future change makes some column non-zero. Because `dropDuplicates()`
  runs over the whole Silver frame, it also reduces the `_unknown` partition to
  those 148 distinct malformed rows. Every original copy stays in Bronze.
