{#
    Gaussian rather than Savitzky-Golay: hours per week drop to zero on
    holidays, and savgol's negative lobes would ring below zero around the
    spikes either side of a break. Hours are always positive.
#}
{% set sigma = 4 %}
{% set truncate = 3 %}

WITH smoothed AS (
    {{ gaussian_smooth(
        relation=ref('core_gcal__weekly_work'),
        measure='duration_hours',
        dims=['calendar_name'],
        sigma=sigma,
        truncate=truncate,
        out_column='trend_hours',
        grain='week'
    ) }}
),

final AS (
    SELECT
        -------- dims
        week_date,
        calendar_name,

        -------- measures
        duration_hours,
        trend_hours,

        -------- metadata
        is_filled,
        week_date > (SELECT max(week_date) FROM smoothed) - INTERVAL {{ sigma * truncate }} WEEK
            AS is_provisional_trend
    FROM smoothed
    ORDER BY ALL
)

SELECT * FROM final
