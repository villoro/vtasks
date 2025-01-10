WITH source AS (
    SELECT *
    FROM {{ ref('base_dropbox__money_lover') }}
)

SELECT *
FROM source
