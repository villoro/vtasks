-- The `total` language must equal the sum of the real languages, both for the
-- raw value and for the smoothed trend. The trend only holds because the
-- Gaussian kernel is linear, so smoothing the sum equals summing the smoothed.

WITH trends AS (
    SELECT * FROM {{ ref('marts_books__monthly_by_language_trends') }}
),

totals AS (
    SELECT month_date, metric, value, trend
    FROM trends
    WHERE is_total
),

summed AS (
    SELECT
        month_date,
        metric,
        sum(value) AS value,
        sum(trend) AS trend
    FROM trends
    WHERE NOT is_total
    GROUP BY ALL
)

SELECT
    totals.month_date,
    totals.metric,
    totals.value AS total_value,
    summed.value AS summed_value,
    totals.trend AS total_trend,
    summed.trend AS summed_trend
FROM totals
FULL OUTER JOIN summed USING (month_date, metric)
WHERE abs(totals.value - summed.value) > 0.05
    OR abs(totals.trend - summed.trend) > 0.05
    OR totals.month_date IS NULL
    OR summed.month_date IS NULL
