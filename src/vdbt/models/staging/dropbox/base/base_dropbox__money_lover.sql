WITH source AS (
    SELECT *
    FROM {{ source('raw__dropbox', 'money_lover') }}
),

casted_and_renamed AS (
    SELECT
        -------- info
        Category AS category,
        abs(amount) AS amount,
        CASE WHEN amount > 0 THEN 'incomes' ELSE 'expenses' END AS transaction_type,
        strptime(Date, '%d/%m/%Y')::DATE AS transaction_date,
        Account AS account,
        Event AS event,
        note AS notes,

        -------- metadata
        _source,
        _exported_at,
        _n_updates
    FROM source
)

SELECT *
FROM casted_and_renamed
