{% set paid_work_calendar = '11_Paid work' %}
{% set work_calendar = '12_Work' %}
{% set min_working_hours = 3 %}

WITH filtered_data AS (
    SELECT *
    FROM {{ ref('marts_gcal__daily_stats') }}
    WHERE (
        calendar_name = '{{ paid_work_calendar }}'
        AND duration_hours > {{ min_working_hours }}
    ) OR calendar_name = '{{ work_calendar }}'
),

with_moving_averages AS (
    SELECT
        start_day,
        calendar_name,
        duration_hours AS total_hours,
        AVG(duration_hours) OVER win_7d AS mavg_7d,
        AVG(duration_hours) OVER win_30d AS mavg_30d,
        quantile_cont(duration_hours, 0.10) OVER win_30d AS p10_30d,
        quantile_cont(duration_hours, 0.90) OVER win_30d AS p90_30d
    FROM filtered_data
    WINDOW 
        win_7d AS (
            PARTITION BY calendar_name
            ORDER BY start_day
            ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING  
        ),
        win_30d AS (
            PARTITION BY calendar_name
            ORDER BY start_day
            ROWS BETWEEN 15 PRECEDING AND 15 FOLLOWING  
        )
    ORDER BY 1 DESC
),

rounded AS (
    SELECT
        start_day,
        calendar_name,
        ROUND(total_hours, 2) AS total_hours,
        ROUND(mavg_7d, 2) AS mavg_7d,
        ROUND(mavg_30d, 2) AS mavg_30d,
        ROUND(p10_30d, 2) AS p10_30d,
        ROUND(p90_30d, 2) AS p90_30d
    FROM with_moving_averages
)

SELECT *
FROM rounded
