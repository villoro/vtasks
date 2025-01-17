WITH events AS (
    SELECT *
    FROM {{ ref('stg_gcal__events') }}
),

daily_stats AS (
    SELECT
        -------- dims
        started_at :: date AS start_day,
        calendar_name,

        -------- measures
        sum(duration_hours) AS duration_hours
    FROM events
    WHERE NOT is_whole_day_event
    GROUP BY ALL
    ORDER BY ALL
)

SELECT *
FROM daily_stats
