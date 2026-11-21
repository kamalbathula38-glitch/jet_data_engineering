# Architecture: XKCD API to analytics

Left to right: Python ingestion lands raw data, dbt models stage and publish it as a
dimension and fact table, ready for analytics to query.

```mermaid
flowchart LR
    subgraph ingestion["Ingestion (Python)"]
        direction LR
        API["XKCD API<br/>external API, source system<br/>one record per comic<br/>natural key: comic_num"]
        EXTRACT["extract<br/>ingestion framework<br/>modes: incr / hist / backfill<br/>streamed + validated + published<br/>in batch_size chunks"]
        RAW[("raw_xkcd<br/>landing table (stored)<br/>raw, untransformed source rows")]
        API --> EXTRACT --> RAW
    end

    subgraph transform["Transformation (dbt)"]
        direction LR
        STG["stg_xkcd<br/>dbt model, view<br/>trim + null-out blanks<br/>pass-through, no stored grain"]
        DIM["dim_comic<br/>dbt model, dimension<br/>incremental upsert, PK comic_num<br/>title, image_url, alt_text,<br/>transcript, published_date, ingested_at"]
        FACT["fact_comic_metrics<br/>dbt model, fact<br/>derives letters-in-title,<br/>random views / review score<br/>PK+FK comic_num"]
        STG --> DIM
        STG --> FACT
        DIM -. comic_num .-> FACT
    end

    subgraph analytics["Analytics"]
        CONSUME["Analytics consumption<br/>dim_comic join fact_comic_metrics<br/>on comic_num<br/>grain: 1 metrics row per comic"]
    end

    RAW --> STG
    DIM --> CONSUME
    FACT --> CONSUME

    classDef ingest fill:#cfe0fb,stroke:#5a7fb8,color:#1a2a44;
    classDef xform fill:#cdf0da,stroke:#4f9e6b,color:#133a22;
    classDef out fill:#ded3f7,stroke:#8a6fc9,color:#2a1a44;
    class API,EXTRACT,RAW ingest;
    class STG,DIM,FACT xform;
    class CONSUME out;
```

`raw_xkcd` is the ingestion framework's landing table (Python) - see
[README.md](../../README.md) for the `incr`/`hist`/`backfill` modes, retry/backoff
behavior, and the Airflow DAG (`dags/xkcd_pipeline_dag.py`) that runs this whole flow
end to end on XKCD's publishing schedule. `stg_xkcd` through `fact_comic_metrics` are
dbt models; see [erd.md](erd.md) for the resulting data warehouse model and
[data-quality.md](data-quality.md) for the checks defined at each stage.

`extract` doesn't fetch every comic into memory before writing anything: it streams
records out one at a time, and the ingestion framework groups them into
`loading.batch_size` chunks, validating and publishing each chunk before moving on to
the next. A failure partway through a large `hist`/`backfill` run only costs the current
chunk - everything validated and published before it is already safely in `raw_xkcd`,
and re-running is safe since the destination mode is `upsert`.
