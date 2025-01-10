WITH source AS (
    SELECT *
    FROM {{ ref('base_dropbox__money_lover') }}
),

invalid_categories AS (
    SELECT *
    FROM {{ ref('invalid_categories') }}
),

without_invalid_categories AS (
    SELECT *
    FROM source
    LEFT JOIN invalid_categories USING (category)
    WHERE invalid_categories.category IS NULL
),

without_fravi_incomes AS (
    SELECT *
    FROM without_invalid_categories
    WHERE NOT (account = 'FraVi' AND transaction_type = 'incomes')
)

SELECT *
FROM without_fravi_incomes
