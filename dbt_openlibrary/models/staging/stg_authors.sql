{{ config(
    materialized = 'table'
) }}

WITH source AS (

    SELECT
        *
    FROM
        {{ source(
            'openlibrary_data',
            'openlibrary_author'
        ) }}
),
renamed AS (
    SELECT
        key AS ol_author_key,
        name,
        bio,
        birth_date,
        book_key AS ol_book_key
    FROM
        source
)
SELECT
    *
FROM
    renamed
