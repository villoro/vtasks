WITH source AS (
    SELECT *
    FROM {{ ref('base_gsheets__categories') }}
)

SELECT * FROM source
