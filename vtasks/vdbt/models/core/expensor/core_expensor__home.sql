WITH source AS (
    SELECT * FROM {{ ref('stg_gsheets__mortgage') }}
),

renamed AS (
    SELECT
        -------- pks
        month AS change_date,

        -------- measures
        equity_personal AS home_equity,
        debt_personal AS home_debt,
        home_value_personal AS home_value,

        -------- metadata
        _source,
        _exported_at,
        _n_updates
    FROM source
),

unpivoted AS (
    UNPIVOT renamed
    ON home_equity, home_debt, home_value
    INTO
    NAME account_name
    VALUE value_eur
),

final AS (
    SELECT
        -------- pks
        change_date,
        'home' AS account_type,
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
