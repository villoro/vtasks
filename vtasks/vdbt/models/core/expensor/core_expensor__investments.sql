WITH source AS (
    SELECT * FROM {{ ref('stg_gsheets__investments') }}
),

unpivoted AS (
    UNPIVOT source
    ON {{ var('expensor')['accounts']['investments'] | join(', ') }}
    INTO
    NAME account_name
    VALUE value_eur
),

final AS (
    SELECT
        -------- pks
        change_date,
        account_type,
        account_name,

        -------- measures
        value_eur,

        -------- metadata
        _source,
        _exported_at,
        _n_updates
    FROM unpivoted
)

SELECT * FROM final