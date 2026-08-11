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

-- The grid expands over the (transaction_type, category) pairs that actually
-- exist, not their cross product, so income categories never appear under
-- expenses. Holes are the norm here: most categories have months with no
-- transaction at all.
grid AS (
    {{ monthly_grid(
        relation='by_category',
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
        is_filled
    FROM grid
    WHERE month_date <= date_trunc('month', CURRENT_DATE)
    ORDER BY ALL
)

SELECT * FROM final
