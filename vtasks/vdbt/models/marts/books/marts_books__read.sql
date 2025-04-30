WITH source AS (
    SELECT *
    FROM {{ ref('stg_gsheets__books_read') }}
)

SELECT *
FROM source
