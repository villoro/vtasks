{#
    Gaussian rather than Savitzky-Golay: books finished per month is a sparse,
    spiky, non-negative series, and the negative lobes of a savgol filter would
    ring below zero around an isolated month with several books.
#}
{% set sigma = 3 %}
{% set truncate = 3 %}

WITH smoothed AS (
    {{ gaussian_smooth(
        relation=ref('core_books__monthly'),
        measure='value',
        dims=['metric'],
        sigma=sigma,
        truncate=truncate,
        out_column='trend'
    ) }}
),

final AS (
    SELECT
        -------- dims
        month_date,
        metric,

        -------- measures
        value,
        trend,

        -------- metadata
        is_filled,
        -- The kernel is centred, so the newest months only see half of it and
        -- their trend keeps moving as data arrives. Relative to the end of the
        -- series rather than to today, so it follows the data if reading is
        -- logged late.
        month_date > (SELECT max(month_date) FROM smoothed) - INTERVAL {{ sigma * truncate }} MONTH
            AS is_provisional_trend
    FROM smoothed
    ORDER BY ALL
)

SELECT * FROM final
