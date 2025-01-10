WITH source AS (
    SELECT *
    FROM {{ source('raw__gsheets', 'books_read') }}
),

casted_and_renamed AS (
    SELECT
        -------- info
        Title AS title,
        Author AS author,
        Language AS language,
        Type AS type,

        -------- time related
        Date :: date AS read_date,

        -------- measures
        Pages AS num_pages,

        -------- metadata
        _source,
        _exported_at,
        _n_updates
    FROM source
)

SELECT *
FROM casted_and_renamed
