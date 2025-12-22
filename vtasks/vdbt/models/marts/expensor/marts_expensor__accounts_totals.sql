WITH account_values AS (
    SELECT * FROM {{ ref('marts_expensor__accounts_values') }}
),

transactions AS (
    SELECT * FROM {{ ref('marts_expensor__transactions') }}
),

invested AS (
    SELECT
        change_date,
        'financial_invested' AS category,
        sum(value_eur) AS value_eur,
    FROM account_values
    WHERE account_type IN ('liquid', 'financial_invested')
    GROUP BY ALL
),

assets AS (
    SELECT
        change_date,
        'financial_assets' AS category,
        sum(value_eur) AS value_eur,
    FROM account_values
    WHERE account_type IN ('liquid', 'financial_assets')
    GROUP BY ALL
),

home AS (
    SELECT
        change_date,
        account_name AS category,
        sum(value_eur) AS value_eur,
    FROM account_values
    WHERE account_type = 'home' AND account_name IN ('home_equity', 'home_debt')
    GROUP BY ALL
),

monthly_transactions AS (
    SELECT
        date_trunc('month', transaction_date) AS change_date,
        transaction_type AS category,
        sum(personal_amount) AS _value
    FROM transactions
    GROUP BY ALL
),

cumsum_transactions AS (
    SELECT
        change_date,
        category,
        sum(_value) OVER (PARTITION BY category ORDER BY change_date) AS value_eur
    FROM monthly_transactions
),

combined AS (
    SELECT * FROM invested UNION ALL BY NAME
    SELECT * FROM assets UNION ALL BY NAME
    SELECT * FROM home UNION ALL BY NAME
    SELECT * FROM cumsum_transactions
)

SELECT *
FROM combined
ORDER BY ALL
