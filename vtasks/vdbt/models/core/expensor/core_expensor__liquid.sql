WITH source AS (
    SELECT *
    FROM {{ ref('stg_gsheets__liquids') }}
),

un_pivoted AS (
    UNPIVOT source
    ON {{ var('expensor')['accounts']['liquid'] | join(', ') }}
    INTO
    NAME account_name
    VALUE value_eur
),

ordered_cols AS (
    SELECT
        -------- pks
        change_date,
        'liquid' AS account_type,
        account_name,

        -------- measures
        value_eur,

        -------- metadata
        _source,
        _exported_at,
        _n_updates
    FROM un_pivoted
)

SELECT *
FROM ordered_cols
