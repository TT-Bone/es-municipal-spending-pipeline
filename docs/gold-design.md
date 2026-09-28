# Gold Layer Design

Three tables, one per question in the README. All share the grain
**municipality × reference year**, and all exclude the `_unknown` provider: rows
with no municipality cannot be compared across municipalities.

Output: `data/lake/gold/<table>/` (Parquet).

## `spending_by_municipality_year`

Net committed (`empenhada`), liquidated and paid amounts per municipality and
year. Each source row is one month of one empenho, so summing over the year
yields the net figure; reversals are negative rows and net out.

Comparability without population data (out of scope): `state_share` (share of
the state's committed total that year) and `rank_in_year`.

## `carryover_rate_by_municipality_year`

`carryover_rate = (empenhada − paga) / empenhada`, restricted to empenhos issued
in the reference year itself (`AnoEmpenho = AnoReferencia`). The rate is null,
not 0, when nothing was committed.

Two decisions, both checked on the development sample:

- **Own-year filter.** `Paga` also carries payments of prior-year empenhos
  (e.g. 96,403.46 paid in 2024 against 2023 empenhos). Including them would
  offset the current year's unpaid balance with an unrelated year's payments.
- **The source's `RPNPExercicio` / `RPPExercicio` are not used.** They appear
  only in month 12 and capture a tiny fraction of the unpaid balance: 0.00 in
  2023 against 364,787.92 unpaid, and 3,230.00 in 2024 against 1,524,456.12.
  They likely reflect only balances formally inscribed as carry-over, which is
  not the same as "committed and not paid within the year". This was verified on
  one municipality only; re-check on the full dataset before relying on it.

## `supplier_concentration_by_municipality_year`

Share of committed spending going to the top 10 creditors (`top_n_share`), plus
`creditor_count`.

- **Creditor key:** `CodigoCredorFornecedor`, falling back to
  `NomeCredorFornecedor`. Individuals (`NomeIdentificacao = CPF`) arrive with a
  null code, so name is the only identifier available for them.
- **Generic budget buckets are excluded from the ranking.** Creditors with
  `NomeIdentificacao` starting `INSCRIÇÃO GENÉRICA` are payroll and budget
  lines (e.g. `PESSOAL CIVIL - SAÚDE ...`), not suppliers. Left in, they would
  dominate the top 10 and measure nothing. Their weight is reported as
  `generic_empenhada` so the exclusion is visible rather than silent; in the
  sample they are 22–28% of the municipality's spending.
- Creditors with non-positive net commitments are dropped from the ranking.
- Names are matched exactly; the same person spelled differently is counted
  twice. Accepted as a limitation.
