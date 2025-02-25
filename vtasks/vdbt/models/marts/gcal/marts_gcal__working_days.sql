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
        AVG(duration_hours) OVER (
            PARTITION BY calendar_name
            ORDER BY start_day
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS moving_avg_7_days,
        AVG(duration_hours) OVER (
            PARTITION BY calendar_name
            ORDER BY start_day
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS moving_avg_30_days
    FROM filtered_data
    ORDER BY 1 DESC
)

SELECT *
FROM with_moving_averages
