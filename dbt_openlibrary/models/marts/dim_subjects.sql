WITH stg_books AS (
    SELECT
        *
    FROM
        {{ ref('stg_books') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['stg_books.genre']) }} AS subject_id,
    genre AS subject
FROM
    stg_books
GROUP BY
    genre
