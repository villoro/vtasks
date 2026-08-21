WITH transactions AS (
    SELECT * FROM {{ ref('marts_expensor__transactions') }}
    WHERE NOT is_excluded
),

by_type AS (
    SELECT
        date_trunc('month', transaction_date) AS month_date,
        transaction_type AS category,
        sum(personal_amount) AS value_eur
    FROM transactions
    GROUP BY ALL
),

-- `result` is incomes - expenses. Since the smoother is a linear filter,
-- smoothing the result equals smoothing incomes minus smoothing expenses,
-- so it can safely be built here as just another category.
result AS (
    SELECT
        month_date,
        'result' AS category,
        sum(CASE WHEN category = 'incomes' THEN value_eur ELSE -value_eur END) AS value_eur
    FROM by_type
    GROUP BY ALL
),

combined AS (
    SELECT * FROM by_type UNION ALL BY NAME
    SELECT * FROM result
),

-- Fills the holes: months with no income at all, and later on every
-- category-month with no transaction.
grid AS (
    {{ date_grid(
        relation='combined',
        date_column='month_date',
        measure='value_eur',
        dims=['category'],
        fill='zero'
    ) }}
),

final AS (
    SELECT
        -------- dims
        month_date,
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
