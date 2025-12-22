WITH source AS (
    SELECT *
    FROM {{ dbt_utils.union_relations(
        relations=[
            ref('core_expensor__home'),
            ref('core_expensor__investments'),
            ref('core_expensor__liquid'),
        ])
    }}
),

accounts AS (
    SELECT * FROM {{ ref('stg_gsheets__accounts') }}
),

with_account_subtypes AS (
    SELECT
        source.*,
        accounts.account_subtype
    FROM source
    JOIN accounts ON lower(source.account_name) = lower(accounts.name)
),

selected_columns AS (
    SELECT
        -------- pks
        change_date,
        account_type,
        account_name,

        -------- account details
        account_subtype,

        -------- measures
        value_eur,

        -------- metadata
        _source,
        _exported_at,
        _n_updates
    FROM with_account_subtypes
    ORDER BY ALL
)

SELECT * FROM selected_columns
