WITH source AS (
    SELECT * FROM {{ ref('base_gsheets__liquids') }}
)

SELECT * FROM source
