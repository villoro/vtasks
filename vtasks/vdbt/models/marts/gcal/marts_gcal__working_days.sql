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
        duration_hours AS total_hours,
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
),

values_rounded AS (
    SELECT
        -------- dims
        start_day,

        -------- measures
        round(total_hours, 2) AS total_hours,
        round(moving_avg_7_days, 2) AS moving_avg_7_days,
        round(moving_avg_30_days, 2) AS moving_avg_30_days
    FROM with_moving_averages
)

SELECT *
FROM values_rounded
