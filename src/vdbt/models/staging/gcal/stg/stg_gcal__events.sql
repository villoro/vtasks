WITH source AS (
    SELECT *
    FROM {{ ref('base_gcal__events') }}
)

SELECT *
FROM source
