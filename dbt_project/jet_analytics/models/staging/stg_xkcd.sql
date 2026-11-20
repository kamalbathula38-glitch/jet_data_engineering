SELECT
    comic_num,
    TRIM(title) AS title,
    image_url,
    NULLIF(TRIM(alt_text), '') AS alt_text,
    NULLIF(TRIM(transcript), '') AS transcript,
    published_date,
    ingested_at
FROM {{ source('raw', 'raw_xkcd') }}