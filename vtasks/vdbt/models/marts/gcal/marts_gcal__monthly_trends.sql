{#
    Gaussian rather than Savitzky-Golay: hours per calendar is sparse, calendars
    start and stop being used, and hours are always positive so the trend must
    be too.
#}
{% set sigma = 3 %}
{% set truncate = 3 %}

WITH closed_months AS (
    SELECT *
    FROM {{ ref('core_gcal__monthly') }}
    WHERE month_date < date_trunc('month', CURRENT_DATE)
),

smoothed AS (
    {{ gaussian_smooth(
        relation='closed_months',
        measure='duration_hours',
        dims=['calendar_name'],
        sigma=sigma,
        truncate=truncate,
        out_column='trend_hours'
    ) }}
),

final AS (
    SELECT
        -------- dims
        month_date,
        calendar_name,

        -------- measures
        duration_hours,
        trend_hours,

        -------- metadata
        is_filled,
        month_date > (SELECT max(month_date) FROM smoothed) - INTERVAL {{ sigma * truncate }} MONTH
            AS is_provisional_trend
    FROM smoothed
    ORDER BY ALL
)

SELECT * FROM final
