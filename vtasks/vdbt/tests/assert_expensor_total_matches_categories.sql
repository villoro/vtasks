-- The `total` category must equal the sum of the real categories, both for the
-- raw value and for the smoothed trend. The trend only holds because the
-- Gaussian kernel is linear, so smoothing the sum equals summing the smoothed.

WITH trends AS (
    SELECT * FROM {{ ref('marts_expensor__monthly_by_category_trends') }}
),

totals AS (
    SELECT month_date, transaction_type, value_eur, trend_eur
    FROM trends
    WHERE is_total
),

summed AS (
    SELECT
        month_date,
        transaction_type,
        sum(value_eur) AS value_eur,
        sum(trend_eur) AS trend_eur
    FROM trends
    WHERE NOT is_total
    GROUP BY ALL
)

SELECT
    totals.month_date,
    totals.transaction_type,
    totals.value_eur AS total_value,
    summed.value_eur AS summed_value,
    totals.trend_eur AS total_trend,
    summed.trend_eur AS summed_trend
FROM totals
FULL OUTER JOIN summed USING (month_date, transaction_type)
WHERE abs(totals.value_eur - summed.value_eur) > 0.05
    OR abs(totals.trend_eur - summed.trend_eur) > 0.05
    OR totals.month_date IS NULL
    OR summed.month_date IS NULL
