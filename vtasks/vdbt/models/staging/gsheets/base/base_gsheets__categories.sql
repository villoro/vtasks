WITH source AS (
    SELECT *
    FROM {{ source('raw__gsheets', 'categories') }}
),

casted_and_renamed AS (
    SELECT
        -------- pk
        Name AS name,

        -------- info
        lower(Type) AS category_type,
        "Color Name" AS color_name,
        "Color Index" AS color_index,
        Color AS color,

        -------- flags
        Fixed :: boolean AS is_fixed,

        -------- metadata
        _source,
        _exported_at,
        _n_updates
    FROM source
)

SELECT *
FROM casted_and_renamed
