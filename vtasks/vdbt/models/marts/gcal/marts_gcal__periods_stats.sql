{# Define base dates #}
{% set base_dates = {
    'week': "DATE_TRUNC('week', CURRENT_DATE)",
    'month': "DATE_TRUNC('month', CURRENT_DATE)",
    'year': "DATE_TRUNC('year', CURRENT_DATE)"
} %}

{# Define current time windows #}
{% set current_windows = [
    ('current_week', base_dates['week']),
    ('current_month', base_dates['month']),
    ('current_year', base_dates['year'])
] %}

{# Define last time windows #}
{% set last_windows = [
    ('last_week', base_dates['week'] + " - INTERVAL 7 DAY", base_dates['week']),
    ('last_month', base_dates['month'] + " - INTERVAL 1 MONTH", base_dates['month']),
    ('last_year', base_dates['year'] + " - INTERVAL 1 YEAR", base_dates['year'])
] %}

WITH daily_stats AS (
    SELECT *
    FROM {{ ref('marts_gcal__daily_stats') }}
),

time_aggregations AS (
    {# Loop for current periods #}
    {% for window_name, start_date in current_windows %}
    SELECT 
        calendar_name,
        '{{ window_name }}' AS time_window,
        SUM(duration_hours) AS total_duration,
        DATE_DIFF('day', {{ start_date }}, CURRENT_DATE) + 1 AS num_days
    FROM daily_stats
    WHERE start_day >= {{ start_date }}
    GROUP BY calendar_name
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}

    UNION ALL

    {# Loop for last periods #}
    {% for window_name, start_date, end_date in last_windows %}
    SELECT 
        calendar_name,
        '{{ window_name }}' AS time_window,
        SUM(duration_hours) AS total_duration,
        DATE_DIFF('day', {{ start_date }}, {{ end_date }}) AS num_days
    FROM daily_stats
    WHERE start_day >= {{ start_date }} AND start_day < {{ end_date }}
    GROUP BY calendar_name
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
)

SELECT *
FROM time_aggregations
