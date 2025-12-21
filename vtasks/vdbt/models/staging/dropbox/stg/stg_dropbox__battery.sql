WITH source AS (
    SELECT * FROM {{ ref('base_dropbox__battery') }}
)

SELECT * FROM source
