WITH source AS (
    SELECT *
    FROM {{ dbt_utils.union_relations(
        relations=[
            ref('core_expensor__investments'),
            ref('core_expensor__liquid'),
        ])
    }}
),

selected_columns AS (
    SELECT
        -------- pks
        change_date,
        investment_type,
        account_name,

        -------- measures
        value_eur,

        -------- metadata
        _source,
        _exported_at,
        _n_updates
    FROM source
    ORDER BY ALL
)

SELECT *
FROM selected_columns
