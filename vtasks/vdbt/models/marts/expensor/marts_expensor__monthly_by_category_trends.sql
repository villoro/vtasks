{#
    Gaussian rather than Savitzky-Golay: per category the series is sparse and
    spiky, and savgol negative lobes ring below zero around an isolated large
    transaction. Amounts are always positive here, so the trend must be too.
#}

{% set sigma = 3 %}
{% set truncate = 3 %}

WITH closed_months AS (
    SELECT *
    FROM {{ ref('core_expensor__monthly_by_category') }}
    WHERE month_date < date_trunc('month', CURRENT_DATE)
),

smoothed AS (
    {{ gaussian_smooth(
        relation='closed_months',
        measure='value_eur',
        dims=['transaction_type', 'category'],
        sigma=sigma,
        truncate=truncate,
        out_column='trend_eur'
    ) }}
),

final AS (
    SELECT
        -------- dims
        month_date,
        transaction_type,
        category,

        -------- measures
        value_eur,
        trend_eur,

        -------- metadata
        is_total,
        is_filled,
        month_date > (SELECT max(month_date) FROM smoothed) - INTERVAL {{ sigma * truncate }} MONTH
            AS is_provisional_trend
    FROM smoothed
    ORDER BY ALL
)

SELECT * FROM final
