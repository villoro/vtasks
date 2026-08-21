{% set paid_work_calendar = '11_Paid work' %}
{% set work_calendar = '12_Work' %}

WITH daily_stats AS (
    SELECT * FROM {{ ref('marts_gcal__daily_stats') }}
    WHERE calendar_name IN ('{{ paid_work_calendar }}', '{{ work_calendar }}')
),

by_calendar AS (
    SELECT
        date_trunc('week', start_day) AS week_date,
        calendar_name,
        sum(duration_hours) AS duration_hours
    FROM daily_stats
    GROUP BY ALL
),

-- Weeks with no work at all, holidays and time off, have to be present as 0
-- rather than missing, otherwise the smoother treats the weeks either side of
-- a break as consecutive.
grid AS (
    {{ date_grid(
        relation='by_calendar',
        date_column='week_date',
        measure='duration_hours',
        dims=['calendar_name'],
        fill='zero',
        grain='week'
    ) }}
),

final AS (
    SELECT
        -------- dims
        week_date,
        calendar_name,

        -------- measures
        round(duration_hours, 2) AS duration_hours,

        -------- metadata
        is_filled
    FROM grid
    -- The current week is still in progress, so its total is always partial
    WHERE week_date < date_trunc('week', CURRENT_DATE)
    ORDER BY ALL
)

SELECT * FROM final
