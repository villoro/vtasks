WITH source AS (
    SELECT * FROM {{ source('raw__dropbox', 'battery_pixel_4a') }}
),

casted_and_renamed AS (
    SELECT
        -------- time related
        TO_TIMESTAMP(time :: BIGINT / 1000) AS measured_at,

        -------- measures
        REGEXP_EXTRACT(battery, '(\d+)%').REPLACE('%', '') :: INT AS battery_percent,
        REGEXP_EXTRACT(ma, '(\d+)') :: INT AS ma,
        REGEXP_EXTRACT(mv, '(\d+)') :: INT AS mv,
        REGEXP_EXTRACT(change, '(-?\d+)') :: INT AS change,

        -------- booleans
        LOWER(screen) = 'on' AS is_screen_on,
        LOWER(restart) = 'restart' AS was_restarted,

        -------- other
        plugged,

        -------- metadata
        _source,
        _exported_at,
        _n_updates
    FROM source
)

SELECT * FROM casted_and_renamed
