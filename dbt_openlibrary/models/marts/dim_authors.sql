WITH stg_authors AS (
    SELECT
        *
    FROM
        {{ ref('stg_authors') }}
)
SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['stg_authors.ol_author_key']) }} AS author_id,
    ol_author_key,
    name,
    bio,
    birth_date
FROM
    stg_authors
