{% set fra_cal = 'fra.work' %}

WITH source AS (
    SELECT * FROM {{ source('raw__gcal', 'events') }}
),

casted_ts AS (
    SELECT
        *,
        "start" :: timestamp AS started_at,
        "end" :: timestamp AS ended_at
    FROM source
),

casted_and_renamed AS (
    SELECT
        -------- pk
        id,

        -------- time related
        started_at,
        ended_at,
        EXTRACT(EPOCH FROM age(ended_at, started_at)) / 3600 AS duration_hours,

        -------- booleans
        len("start") == 10 AS is_whole_day_event,
        calendar != '{{ fra_cal }}' AS is_personal,

        -------- info
        nullif(summary, 'nan') AS title,
        nullif(calendar, 'nan') AS calendar_name,
        nullif(location, 'nan') AS location,
        nullif(description, 'nan') AS description,

        -------- metadata
        _exported_at,
        _n_updates
    FROM casted_ts
)

SELECT * FROM casted_and_renamed
