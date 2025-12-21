WITH source AS (
    SELECT * FROM {{ ref('base_gsheets__books_read') }}
)

SELECT * FROM source
