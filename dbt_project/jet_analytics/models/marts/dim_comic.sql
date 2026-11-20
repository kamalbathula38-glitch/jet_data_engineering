{{
    config(
        materialized='incremental',
        unique_key='comic_num',
        incremental_strategy='delete+insert',
    )
}}

SELECT
    comic_num,
    title,
    image_url,
    alt_text,
    transcript,
    published_date,
    ingested_at,
    now() AS dbt_loaded_at
FROM {{ ref('stg_xkcd') }}

{% if is_incremental() %}
WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
{% endif %}