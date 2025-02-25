WITH source AS (
    SELECT *
    FROM {{ ref('base_gsheets__salaries') }}
)

SELECT *
FROM source
