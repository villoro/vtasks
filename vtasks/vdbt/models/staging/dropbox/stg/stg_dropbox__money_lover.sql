WITH source AS (
    SELECT * FROM {{ ref('base_dropbox__money_lover') }}
),

invalid_categories AS (
    SELECT * FROM {{ ref('invalid_categories') }}
),

without_dups AS (
    SELECT *
    FROM source
    WHERE _exported_at = (SELECT MAX(_exported_at) FROM source)
),

without_invalid_categories AS (
    SELECT *
    FROM without_dups
    LEFT JOIN invalid_categories USING (category)
    WHERE invalid_categories.category IS NULL
)

SELECT * FROM without_invalid_categories
