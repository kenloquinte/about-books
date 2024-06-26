WITH stg_authors AS (
    SELECT
        *
    FROM
        {{ ref('stg_authors') }}
),
stg_books AS (
    SELECT
        *
    FROM
        {{ ref('stg_books') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['stg_books.genre']) }} AS subject_id,
    {{ dbt_utils.generate_surrogate_key(['stg_books.ol_book_key']) }} AS book_id,
    {{ dbt_utils.generate_surrogate_key(['stg_authors.ol_author_key']) }} AS author_id,
    1 AS record_count
FROM
    stg_authors
    INNER JOIN stg_books
    ON stg_authors.ol_book_key = stg_books.ol_book_key
