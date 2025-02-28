{% set work_calendar = '11_Paid work' %}
{% set min_working_hours = 3 %}

WITH filtered_data AS (
    SELECT *
    FROM {{ ref('marts_gcal__daily_stats') }}
    WHERE
        calendar_name = '{{ work_calendar }}'
        AND duration_hours > {{ min_working_hours }}
),

with_moving_averages AS (
    SELECT
        start_day,
        ROUND(duration_hours, 2) AS total_hours,
        ROUND(AVG(duration_hours) OVER win_7d, 2) AS moving_avg_7_days,
        ROUND(AVG(duration_hours) OVER win_30d, 2) AS moving_avg_30_days
    FROM filtered_data
    WINDOW 
        win_7d AS (
            PARTITION BY calendar_name
            ORDER BY start_day
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ),
        win_30d AS (
            PARTITION BY calendar_name
            ORDER BY start_day
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )
)

SELECT *
FROM with_moving_averages
