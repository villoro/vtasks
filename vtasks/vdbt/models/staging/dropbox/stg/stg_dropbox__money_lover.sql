WITH source AS (
    SELECT *
    FROM {{ ref('int_dropbox__money_lover') }}
)

SELECT *
FROM source
