WITH source AS (
    SELECT *
    FROM {{ ref('base_gsheets__investments') }}
)

SELECT *
FROM source
