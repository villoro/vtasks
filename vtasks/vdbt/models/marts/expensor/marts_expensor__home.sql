WITH source AS (
    SELECT * FROM {{ ref('stg_dropbox__money_lover') }}
    WHERE lower(account) = 'home'
),

final AS (
    SELECT
        category,
        amount,
        transaction_date,
        notes
    FROM source
)

SELECT * FROM final
