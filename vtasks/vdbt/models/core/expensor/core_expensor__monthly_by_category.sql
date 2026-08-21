WITH transactions AS (
    SELECT * FROM {{ ref('marts_expensor__transactions') }}
    WHERE NOT is_excluded
),

by_category AS (
    SELECT
        date_trunc('month', transaction_date) AS month_date,
        transaction_type,
        category,
        sum(personal_amount) AS value_eur
    FROM transactions
    GROUP BY ALL
),

-- Carried as an extra `category` per transaction_type so a chart gets one
-- trace per category plus the total from a single breakout. Smoothed as its
-- own series, which for a linear filter is the same as summing the smoothed
-- categories.
-- Note this is the Gaussian-smoothed total. `marts_expensor__monthly_trends`
-- keeps the savgol one, which tracks turning points faster.
totals AS (
    SELECT
        date_trunc('month', transaction_date) AS month_date,
        transaction_type,
        'Total' AS category,
        sum(personal_amount) AS value_eur
    FROM transactions
    GROUP BY ALL
),

combined AS (
    SELECT * FROM by_category UNION ALL BY NAME
    SELECT * FROM totals
),

-- The grid expands over the (transaction_type, category) pairs that actually
-- exist, not their cross product, so income categories never appear under
-- expenses. Holes are the norm here: most categories have months with no
-- transaction at all.
grid AS (
    {{ date_grid(
        relation='combined',
        date_column='month_date',
        measure='value_eur',
        dims=['transaction_type', 'category'],
        fill='zero'
    ) }}
),

final AS (
    SELECT
        -------- dims
        month_date,
        transaction_type,
        category,

        -------- measures
        round(value_eur, 2) AS value_eur,

        -------- metadata
        category = 'Total' AS is_total,
        is_filled
    FROM grid
    WHERE month_date <= date_trunc('month', CURRENT_DATE)
    ORDER BY ALL
)

SELECT * FROM final
