WITH daily_stats AS (
    SELECT * FROM {{ ref('marts_gcal__daily_stats') }}
    WHERE is_personal
),

by_calendar AS (
    SELECT
        date_trunc('month', start_day) AS month_date,
        calendar_name,
        sum(duration_hours) AS duration_hours
    FROM daily_stats
    GROUP BY ALL
),

-- Calendars come and go, so most of them have months with no event at all.
grid AS (
    {{ monthly_grid(
        relation='by_calendar',
        date_column='month_date',
        measure='duration_hours',
        dims=['calendar_name'],
        fill='zero'
    ) }}
),

final AS (
    SELECT
        -------- dims
        month_date,
        calendar_name,

        -------- measures
        round(duration_hours, 2) AS duration_hours,

        -------- metadata
        is_filled
    FROM grid
    WHERE month_date <= date_trunc('month', CURRENT_DATE)
    ORDER BY ALL
)

SELECT * FROM final
