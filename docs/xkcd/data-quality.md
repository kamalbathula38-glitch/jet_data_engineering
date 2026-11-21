# Data quality checks

Checks are defined at two layers: the ingestion framework validates `raw_xkcd` records
before they're published (schema + `data_quality.expectations` in
`config/ingestions/xkcd.yaml`), and dbt tests validate the warehouse model on every
`dbt build` - including the one the Airflow DAG runs automatically.

Ingestion-layer checks run per `loading.batch_size` chunk, not across the whole run (see
[architecture.md](architecture.md)). That doesn't weaken `not_null`/`min`/`max` - those
give the same verdict whether checked per-record, per-chunk, or across everything. It
does mean a `unique` expectation only catches a duplicate within the same chunk, not one
split across two chunks. For XKCD specifically that gap can't actually be hit: `extract`
builds `comic_num` from a plain, non-repeating range, so no duplicate can occur
regardless of chunking.

| Table | Column | Check | Catches |
|---|---|---|---|
| `raw_xkcd` | `comic_num`, `title`, `image_url` | not_null (schema) | missing required fields from the source |
| `raw_xkcd` | `comic_num` | min >= 1 | a malformed API response ingested as a negative/zero id |
| `dim_comic` | `comic_num` | unique, not_null | duplicate or missing comic identity |
| `dim_comic` | `title`, `image_url` | not_null | incomplete source records |
| `fact_comic_metrics` | `comic_num` | unique, not_null, `relationships` -> dim_comic | a metrics row with no matching comic (orphaned fact) |
| `fact_comic_metrics` | `views` | not_null, between 0-10000 | the random-value formula drifting out of spec |
| `fact_comic_metrics` | `customer_review_score` | not_null, between 1.0-10.0 | same, for the review-score formula |
| `fact_comic_metrics` | `creation_cost_eur` | not_null, between 0-10000 | a broken letter-count/cost derivation |

`dim_comic` and `fact_comic_metrics` are dbt incremental models (`delete+insert`, unique
key `comic_num`). The `relationships` test and the `value_between` range checks
(a custom generic test - see `dbt_project/jet_analytics/tests/generic/`) are defined in
`dbt_project/jet_analytics/models/marts/marts.yml`.
