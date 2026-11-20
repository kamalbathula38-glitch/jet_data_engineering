{{ 
    config(
        materialized='incremental',
        unique_key='comic_num',
        incremental_strategy='delete+insert',
    )
}}

WITH new_comics AS (

    SELECT
        comic_num,
        title,
        ingested_at AS source_loaded_at,

        LENGTH(
            REGEXP_REPLACE(title, '[^[:alpha:]]', '', 'g')
        ) AS title_letter_count

    FROM {{ ref('dim_comic') }}

    {% if is_incremental() %}
    WHERE ingested_at > (SELECT MAX(source_loaded_at) FROM {{ this }})
    {% endif %}

)

SELECT
    comic_num,
    title_letter_count,
    title_letter_count * 5 AS creation_cost_eur,

    FLOOR(RANDOM() * 10001)::INTEGER AS views,

    ROUND(
        (1.0 + RANDOM() * 9.0)::NUMERIC,
        1
    ) AS customer_review_score,

    source_loaded_at,
    now() AS dbt_loaded_at

FROM new_comics
