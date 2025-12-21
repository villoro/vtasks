{%- set accounts = var("expensor")["accounts"]["investments"] -%}

WITH source AS (
    SELECT *
    FROM {{ dbt_utils.union_relations(
        relations=[
            source('raw__gsheets', 'invest_month'),
            source('raw__gsheets', 'worth_month'),
        ])
    }}
),

casted_and_renamed AS (
    SELECT
        -------- pk
        replace(Date, '/', '-').concat('-01') :: date AS change_date,
        CASE
            WHEN _dbt_source_relation LIKE '%invest%' THEN 'invested'
            ELSE 'worth'
        END AS account_type,

        -------- metrics
        {{ gsheet_to_numeric('total') }} :: decimal(10, 2) AS total,
        {% for col in accounts -%}
            {{ gsheet_to_numeric(col) }} :: decimal(10, 2) AS {{ col }},
        {% endfor -%}

        -------- metadata
        _source,
        _exported_at,
        _n_updates
    FROM source
)

SELECT * FROM casted_and_renamed
