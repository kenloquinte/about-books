{{ config(
    materialized = 'table'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'openlibrary_data',
            'openlibrary_books'
        ) }}
),
renamed AS (
    SELECT
        key AS ol_book_key,
        title,
        cover_id AS ol_cover_id,
        first_publish_year AS publish_year,
        genre
    FROM
        source
)
SELECT
    *
FROM
    renamed
