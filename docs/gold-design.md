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

Two decisions behind the choice of `(Empenhada − Paga)` as the numerator:

- **Own-year filter.** `Paga` also carries payments of prior-year empenhos
  (e.g. 96,403.46 paid in 2024 against 2023 empenhos). Including them would
  offset the current year's unpaid balance with an unrelated year's payments.
- **The source's `RPNPExercicio` / `RPPExercicio` are not used.** They appear
  only in month 12. On the full 2023+2024 dataset, their state-wide sum is
  27.77% of own-year `(Empenhada − Paga)` in 2023 and 99.73% in 2024 — the two
  years disagree too much for RP to be a reliable definition. Restricting
  `(Empenhada − Paga)` to the reference year gives one definition that
  behaves consistently across years.

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
  `generic_empenhada` so the exclusion is visible rather than silent. The
  share is highly skewed across the state: state-wide ~6% (2023) and ~11%
  (2024), but per-municipality it ranges from ~0% to above 40% in both years,
  with the median near 0.01%.
- Creditors with non-positive net commitments are dropped from the ranking.
- Names are matched exactly; the same person spelled differently is counted
  twice. Accepted as a limitation.
