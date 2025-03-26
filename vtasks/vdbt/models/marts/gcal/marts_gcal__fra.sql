{% set min_date = '2025-01-01' %}

WITH events AS (
    SELECT *
    FROM {{ ref('stg_gcal__events') }}
    WHERE NOT is_personal AND NOT is_whole_day_event
),

daily_stats AS (
    SELECT
        -------- dims
        started_at :: date AS start_day,

        -------- measures
        round(sum(duration_hours), 2) AS duration_hours
    FROM events
    WHERE started_at :: date > DATE '{{ min_date }}'
    GROUP BY ALL
    ORDER BY ALL
),

final AS (
    SELECT
        start_day,
        duration_hours,
        DATE_TRUNC('week', start_day) AS week,
        STRFTIME(DATE_TRUNC('month', start_day), '%Y-%m') AS month
    FROM daily_stats
    ORDER BY 1
)

SELECT * FROM final
