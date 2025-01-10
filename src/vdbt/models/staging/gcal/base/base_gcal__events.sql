WITH source AS (
    SELECT *
    FROM {{ source('raw__gcal', 'events') }}
),

casted_and_renamed AS (
    SELECT
        -------- pk
        id,

        -------- time related
        "start" :: timestamp AS started_at,
        "end" :: timestamp AS ended_at,

        -------- booleans
        len("start") == 10 AS is_whole_day_event,

        -------- info
        nullif(summary, 'nan') AS title,
        nullif(calendar, 'nan') AS calendar_name,
        nullif(location, 'nan') AS location,
        nullif(description, 'nan') AS description,

        -------- metadata
        _exported_at,
        _n_updates
    FROM source
)

SELECT *
FROM casted_and_renamed
