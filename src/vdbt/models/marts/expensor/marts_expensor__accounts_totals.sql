WITH account_values AS (
    SELECT *
    FROM {{ ref('marts_expensor__accounts_values') }}
),

transactions AS (
    SELECT *
    FROM {{ ref('marts_expensor__transactions') }}
),

invested AS (
    SELECT
        change_date,
        'invested' AS category,
        sum(value_eur) AS value_eur,
    FROM account_values
    WHERE account_type IN ('liquid', 'invested')
    GROUP BY ALL
),

worth AS (
    SELECT
        change_date,
        'worth' AS category,
        sum(value_eur) AS value_eur,
    FROM account_values
    WHERE account_type IN ('liquid', 'worth')
    GROUP BY ALL
), 

incomes AS (
    SELECT
        date_trunc('month', transaction_date) AS change_date,
        'incomes' AS category,
        sum(personal_amount) AS value_eur
    FROM transactions
    WHERE transaction_type = 'incomes'
    GROUP BY ALL
),

expenses AS (
    SELECT
        date_trunc('month', transaction_date) AS change_date,
        'expenses' AS category,
        sum(personal_amount) AS value_eur
    FROM transactions
    WHERE transaction_type = 'expenses'
    GROUP BY ALL
),

combined AS (
    SELECT * FROM invested UNION ALL
    SELECT * FROM worth UNION ALL
    SELECT * FROM incomes UNION ALL
    SELECT * FROM expenses
),

with_cumsum AS (
    SELECT
        *,
        sum(coalesce(value_eur, 0)) OVER (PARTITION BY category ORDER BY change_date)
            AS cum_value_eur
    FROM combined
    ORDER BY ALL
)

SELECT *
FROM with_cumsum
