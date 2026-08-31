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

- **78 distinct municipalities** appear in the data — the full set of
  municipalities in Espírito Santo. Counted as distinct values across all three
  2020 parts, after discarding the repeated header and one malformed row.
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

## Known data quality issues

- At least one row in the 2020 files has the literal value `4` where a
  municipality name belongs, which points to an unescaped delimiter or line break
  inside a quoted field. Row-shape validation is a real check here, not a
  decorative one.
- Headers repeat once per part file and must be skipped on concatenation.
