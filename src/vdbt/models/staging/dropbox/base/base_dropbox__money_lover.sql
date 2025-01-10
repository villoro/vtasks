WITH source AS (
    SELECT *
    FROM {{ source('raw__dropbox', 'money_lover') }}
),

casted_and_renamed AS (
    SELECT
        -------- info
        category,
        abs(amount) AS amount,
        CASE WHEN amount > 0 THEN 'income' ELSE 'expense' END AS transaction_type,
        Date AS transaction_date,
        account,
        event,
        note AS notes,

        -------- metadata
        _source,
        _exported_at,
        _n_updates
    FROM source
)

SELECT *
FROM source
