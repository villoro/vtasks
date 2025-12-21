WITH source AS (
    SELECT * FROM {{ source('raw__gsheets', 'accounts') }}
),

casted_and_renamed AS (
    SELECT
        -------- pk
        Name AS name,

        -------- info
        "Color Name" AS color_name,
        "Color Index" AS color_index,
        lower(Type) AS account_type,
        Subtype AS account_subtype,
        Country AS country,

        -------- metadata
        _source,
        _exported_at,
        _n_updates
    FROM source
)

SELECT * FROM casted_and_renamed
