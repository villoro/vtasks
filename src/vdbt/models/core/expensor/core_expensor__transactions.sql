WITH transactions AS (
    SELECT *
    FROM {{ ref('stg_dropbox__money_lover') }}
),

categories AS (
    SELECT *
    FROM {{ ref('stg_gsheets__categories') }}
),

share_percentages AS (
    SELECT *
    FROM {{ ref('stg_gsheets__share_percentages') }}
),

transactions_with_percent AS (
    SELECT
        transactions.*,
        share_percentages.percent
    FROM transactions
    LEFT JOIN share_percentages
    ON transactions.transaction_date >= share_percentages.start_date
        AND transactions.transaction_date < share_percentages.end_date
),

final AS (
    SELECT
        -------- pks
        trans.id,

        -------- info
        trans.transaction_date,
        trans.amount AS total_amount,
        trans.amount * COALESCE(trans.percent, 1) AS personal_amount,
        trans.percent AS sharing_percentage,
        trans.account,
        trans.event,
        trans.notes,

        -------- category
        trans.category,
        trans.transaction_type,
        cat.is_fixed,

        -------- metadata
        trans._source,
        trans._exported_at,
        trans._n_updates
    FROM transactions_with_percent trans
    LEFT JOIN categories cat
    ON trans.category = cat.name AND trans.transaction_type = cat.category_type
    ORDER BY id
)

SELECT *
FROM final
