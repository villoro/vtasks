WITH source AS (
    SELECT *
    FROM {{ ref('base_gsheets__mortgage') }}
)

SELECT * FROM source
