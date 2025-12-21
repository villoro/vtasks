WITH source AS (
    SELECT * FROM {{ ref('base_gsheets__share_percentages') }}
)

SELECT * FROM source
