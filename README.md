# JET Data Engineering

A YAML-driven ingestion framework (`ingestion/`, `handlers/`, `registries/`) paired with a
dbt transformation layer (`dbt_project/`), built to hold multiple source pipelines side
by side - each one just a config file, a source/destination handler pair, and a set of
dbt models.

**This repo currently ships one pipeline end to end: XKCD comics analytics**, built for
the JET Data Engineering take-home assignment. See the [XKCD Pipeline](#xkcd-pipeline)
section below to run it, or jump straight to the docs:
[architecture](docs/xkcd/architecture.md), [ER diagram](docs/xkcd/erd.md), and
[data quality](docs/xkcd/data-quality.md).

## Requirements

- Python 3.11+
- A PostgreSQL database - run one locally via Docker (no account needed, see below) or point at your own instance (e.g. Neon)
- Docker + Docker Compose, if you want the local Postgres and/or the Airflow automation (not required for a bare `python ingest.py` run against your own database)

## Setup

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
cp .env.example .env
```

`.env.example` already points `DATABASE_URL` at a local Postgres container, so just bring it up:

```bash
docker compose up -d warehouse
```

To use your own Postgres instance instead (e.g. Neon), set `DATABASE_URL` in `.env` to its connection string. Keep `.env` out of Git either way.

## XKCD Pipeline

Fetches XKCD comics from the public API, lands them in Postgres (`raw_xkcd`), and
transforms them into a small dimension/fact warehouse model (`dim_comic` /
`fact_comic_metrics`) with cost, view, and customer-review metrics. Config lives at
`config/ingestions/xkcd.yaml`; the source and destination handlers are
`handlers/xkcd.py` and `handlers/postgres.py`; the dbt models are under
`dbt_project/jet_analytics/models/`.

### Run ingestion

```bash
python ingest.py --config config/ingestions/xkcd.yaml
```

The first incremental run fetches the latest comic because the target table has no watermark yet. Later runs fetch comic numbers greater than the maximum `comic_num` already stored. The destination uses `comic_num` as its primary key and performs an upsert.

### Historical mode

Change `source.mode` from `incr` to `hist` in the YAML to fetch comics from comic 1 through the latest comic. This can make many API requests; use it only when you intentionally want a full historical load.

### Ad-hoc backfill

To (re)ingest a specific comic number range without touching the YAML, pass `--backfill-start` and `--backfill-end`:

```bash
python ingest.py --config config/ingestions/xkcd.yaml --backfill-start 100 --backfill-end 150
```

This overrides `source.mode` to `backfill` for that run only. The end of the range is clamped to the latest published comic. Because the destination mode is `upsert`, backfilling a range that overlaps existing rows is safe and simply updates them.

### Configuration reference

- `source.ingest_type`: selects a source handler from `registries/native_ingestions.py`.
- `source.mode`: `incr`, `hist`, or `backfill` (also settable ad-hoc via `--backfill-start`/`--backfill-end`).
- `source.watermark_column`: destination column used to read the incremental watermark.
- `destination.publish_type`: selects a publisher from `registries/native_publishers.py`.
- `destination.mode`: currently `upsert`.
- `schema.definition`: explicit schema YAML path, relative to the ingestion config file.
- `loading.id_cols`: one or more columns forming the primary/conflict key.
- `loading.batch_size`: how many records are extracted, validated, and published together as one chunk. Extraction streams records one at a time rather than loading everything into memory up front, so a failure partway through a large `hist`/`backfill` run only costs the current chunk - earlier chunks are already validated and published (safe to leave in place, since the destination mode is `upsert`). Omit it to disable chunking entirely - everything is extracted, validated, and published in a single pass.
- `data_quality.expectations`: optional business/data-specific checks beyond schema validation, evaluated per chunk (see [docs/xkcd/data-quality.md](docs/xkcd/data-quality.md) for what that means for checks like `unique`).

### Transformations (dbt)

The dbt project at `dbt_project/jet_analytics` builds `stg_xkcd` -> `dim_comic`/`fact_comic_metrics` from `raw_xkcd`, along with the data quality tests defined in `models/marts/marts.yml` (not_null, unique, a `relationships` test tying the fact table back to `dim_comic`, and range checks on the derived metrics). See [docs/xkcd/data-quality.md](docs/xkcd/data-quality.md) for the full list.

```bash
cd dbt_project/jet_analytics
dbt build
```

`dbt build` runs the models and all tests in one command, failing loudly if any test fails. It uses the profile committed at `dbt_project/jet_analytics/profiles.yml` (dbt checks the current directory before `~/.dbt/`, so that file is what actually gets used, not any personal profile you may have). Its defaults point at the local Docker warehouse (`localhost:5433`), independently of whatever `DATABASE_URL` is in `.env`.

**If you point `DATABASE_URL` at a different database** (e.g. Neon) for ingestion, `dbt build` on the host won't automatically follow - the two are configured separately. Export matching `DBT_HOST`/`DBT_PORT`/`DBT_USER`/`DBT_PASS`/`DBT_DBNAME` environment variables before running `dbt build` so both tools target the same database, or everything you ingest will land somewhere `dbt build` never looks.

### Automation (Airflow)

`dags/xkcd_pipeline_dag.py` runs the end-to-end pipeline on XKCD's own publishing schedule (Monday/Wednesday/Friday). Since comics aren't posted at a fixed time, it polls every 30 minutes for up to 24 hours for a `comic_num` beyond what's already stored, then runs ingestion followed by `dbt build` (transform + tests). See [docs/xkcd/architecture.md](docs/xkcd/architecture.md) for the full data flow.

The whole stack - Airflow (webserver, scheduler, its own metadata DB) and the local Postgres warehouse - is defined in `docker-compose.yaml`:

```bash
docker compose up -d
```

This pulls the official Airflow image and installs this project's lightweight runtime deps plus `dbt-core`/`dbt-postgres` into it via `_PIP_ADDITIONAL_REQUIREMENTS` (an Airflow-documented dev/test-only convenience - fine here, not for production). Airflow itself is deliberately not in `requirements.txt`, since its pinned dependencies tend to conflict with this project's own stack.

Once it's up:

1. Open http://localhost:8080 (user `admin`, password `admin`).
2. Unpause `xkcd_ingestion_pipeline`.

Inside the Airflow containers, `docker-compose.yaml` sets `DATABASE_URL`, `DBT_HOST`, and `DBT_PORT` directly, pointing both ingestion and dbt at the `warehouse` service - regardless of what's in your own `.env`. That means triggering the DAG always runs against the local Dockerized warehouse, while running things directly on the host uses whatever `DATABASE_URL` and `DBT_HOST`/`DBT_PORT` you've set there (see the note above - keep them in sync).

## Adding another pipeline

The framework is source-agnostic by design - adding a second pipeline alongside XKCD means:

1. A new ingestion handler under `handlers/`, registered in `registries/native_ingestions.py` (and a destination handler too, if it's not Postgres).
2. A new config YAML under `config/ingestions/` and schema YAML under `config/schemas/`.
3. New dbt staging models under `models/staging/<source>/` (dbt's own convention is one staging folder per source system; marts can stay organized by business domain rather than source).
4. Docs under `docs/<pipeline>/`, mirroring `docs/xkcd/`.

## Current scope and limitations

This is the first runnable vertical slice. Incremental extraction uses a simple maximum watermark and does not yet implement lookback windows or durable run history.
