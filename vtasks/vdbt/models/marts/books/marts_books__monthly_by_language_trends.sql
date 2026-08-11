{#
    Sparser than the overall books series, since each language only gets a
    share of the months, so the kernel is a bit wider than the sigma of 3 used
    in `marts_books__monthly_trends`.
#}
{% set sigma = 4 %}
{% set truncate = 3 %}

WITH smoothed AS (
    {{ gaussian_smooth(
        relation=ref('core_books__monthly_by_language'),
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
        is_filled,
        month_date > (SELECT max(month_date) FROM smoothed) - INTERVAL {{ sigma * truncate }} MONTH
            AS is_provisional_trend
    FROM smoothed
    ORDER BY ALL
)

SELECT * FROM final
