{#
    Sparser than the overall books series, since each language only gets a
    share of the months, so the kernel is a bit wider than the sigma of 3 used
    in `marts_books__monthly_trends`.
#}
{% set sigma = 4 %}
{% set truncate = 3 %}

WITH closed_months AS (
    SELECT *
    FROM {{ ref('core_books__monthly_by_language') }}
    WHERE month_date < date_trunc('month', CURRENT_DATE)
),

smoothed AS (
    {{ gaussian_smooth(
        relation='closed_months',
        measure='value',
        dims=['language', 'metric'],
        sigma=sigma,
        truncate=truncate,
        out_column='trend'
    ) }}
),

final AS (
    SELECT
        -------- dims
        month_date,
        language,
        metric,

        -------- measures
        value,
        trend,

        -------- metadata
        is_total,
        is_filled,
        month_date > (SELECT max(month_date) FROM smoothed) - INTERVAL {{ sigma * truncate }} MONTH
            AS is_provisional_trend
    FROM smoothed
    ORDER BY ALL
)

SELECT * FROM final
