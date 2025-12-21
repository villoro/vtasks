WITH source AS (
    SELECT *
    FROM {{ ref('base_gsheets__accounts') }}
)

SELECT * FROM source
