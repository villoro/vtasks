{% set time_windows = {
    'current_week': "start_day >= DATE_TRUNC('week', CURRENT_DATE)",
    'current_month': "start_day >= DATE_TRUNC('month', CURRENT_DATE)",
    'current_year': "start_day >= DATE_TRUNC('year', CURRENT_DATE)",
    'last_week': "start_day >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL 7 DAY AND start_day < DATE_TRUNC('week', CURRENT_DATE)",
    'last_month': "start_day >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL 1 MONTH AND start_day < DATE_TRUNC('month', CURRENT_DATE)",
    'last_year': "start_day >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL 1 YEAR AND start_day < DATE_TRUNC('year', CURRENT_DATE)"
} %}

WITH daily_stats AS (
    SELECT *
    FROM {{ ref('marts_gcal__daily_stats') }}
),

time_aggregations AS (
    {# Loop through each time window to generate a UNION ALL query #}
    {% for window, condition in time_windows.items() %}
    SELECT 
        calendar_name,
        '{{ window }}' AS time_window,
        ROUND(SUM(duration_hours), 2) AS total_duration
    FROM daily_stats
    WHERE {{ condition }}
    GROUP BY calendar_name
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
)

SELECT *
FROM time_aggregations
