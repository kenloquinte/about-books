WITH stg_books AS (
    SELECT
        *
    FROM
        {{ ref('stg_books') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['stg_books.ol_book_key']) }} AS books_id,
    ol_book_key,
    title,
    ol_cover_id,
    publish_year
FROM
    stg_books
