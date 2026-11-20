# JET Data Engineering Assignment — Ingestion Framework

A small YAML-driven ingestion framework with an XKCD source handler and PostgreSQL publisher.

## 1. Requirements

- Python 3.11+
- A PostgreSQL database (Neon is supported)

## 2. Setup

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
cp .env.example .env
```

Set `DATABASE_URL` in `.env` to your Neon connection string. Keep `.env` out of Git.

## 3. Run the first ingestion

From the project root:

```bash
python main.py --config config/ingestions/xkcd.yaml
```

The first incremental run fetches the latest comic because the target table has no watermark yet. Later runs fetch comic numbers greater than the maximum `comic_num` already stored. The destination uses `comic_num` as its primary key and performs an upserts.

## 4. Historical mode

Change `source.mode` from `incr` to `hist` in the YAML to fetch comics from comic 1 through the latest comic. This can make many API requests; use it only when you intentionally want a full historical load.

## 5. Ad-hoc backfill

To (re)ingest a specific comic number range without touching the YAML, pass `--backfill-start` and `--backfill-end`:

```bash
python main.py --config config/ingestions/xkcd.yaml --backfill-start 100 --backfill-end 150
```

This overrides `source.mode` to `backfill` for that run only. The end of the range is clamped to the latest published comic. Because the destination mode is `upsert`, backfilling a range that overlaps existing rows is safe and simply updates them.

## 6. Configuration

- `source.ingest_type`: selects a source handler from `registries/native_ingestions.py`.
- `source.mode`: `incr`, `hist`, or `backfill` (also settable ad-hoc via `--backfill-start`/`--backfill-end`).
- `source.watermark_column`: destination column used to read the incremental watermark.
- `destination.publish_type`: selects a publisher from `registries/native_publishers.py`.
- `destination.mode`: currently `upsert`.
- `schema.definition`: explicit schema YAML path, relative to the ingestion config file.
- `loading.id_cols`: one or more columns forming the primary/conflict key.
- `loading.batch_size`: number of records sent per batch.
- `data_quality.expectations`: optional business/data-specific checks beyond schema validation.

## 7. Transformations (dbt)

The dbt project at `dbt_project/jet_analytics` builds `stg_xkcd` -> `dim_comic`/`fact_comic_metrics` from `raw_xkcd`, along with the data quality tests defined in `models/marts/marts.yml` (not_null, unique, a `relationships` test tying the fact table back to `dim_comic`, and range checks on the derived metrics).

```bash
cd dbt_project/jet_analytics
dbt build
```

`dbt build` runs the models and all tests in one command, failing loudly if any test fails.

## 8. Automation (Airflow)

`dags/xkcd_pipeline_dag.py` runs the end-to-end pipeline on XKCD's own publishing schedule (Monday/Wednesday/Friday). Since comics aren't posted at a fixed time, it polls every 30 minutes for up to 24 hours for a `comic_num` beyond what's already stored, then runs ingestion followed by `dbt build` (transform + tests).

Airflow is deliberately not in `requirements.txt` - its pinned dependencies tend to conflict with this project's own stack, so install it separately (`pip install apache-airflow`) in its own environment. To wire the DAG up:

1. Set the `JET_PROJECT_ROOT` environment variable in the Airflow worker's environment to this repository's absolute path.
2. Make sure that environment has a working `.env` and `dbt` on `PATH`.
3. Copy or symlink `dags/xkcd_pipeline_dag.py` into Airflow's configured `dags_folder`.

## 9. Current scope and limitations

This is the first runnable vertical slice. Incremental extraction uses a simple maximum watermark and does not yet implement lookback windows or durable run history.
