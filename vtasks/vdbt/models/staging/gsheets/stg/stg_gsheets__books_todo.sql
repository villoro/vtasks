WITH source AS (
    SELECT * FROM {{ ref('base_gsheets__books_todo') }}
)

SELECT * FROM source
