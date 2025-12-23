{% set percent = 0.7 %}
{% set buy_year = 2026 %}

WITH source AS (
    SELECT * FROM {{ ref('base_gsheets__mortgage') }}
),

ffilled AS (
    SELECT
        * REPLACE (
            LAST_VALUE(home_value IGNORE NULLS)
            OVER (
                ORDER BY month
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS home_value -- ffill
        )
    FROM source
),

equity AS (
    SELECT
        * REPLACE (
            COALESCE(home_value, 0) AS home_value,
            COALESCE(debt, 0) AS debt
        ),
        COALESCE(home_value, 0) - COALESCE(debt, 0) AS equity,
        IF(YEAR(month) >= {{ buy_year }}, {{ percent }}, 1) AS _personal_percent
    FROM ffilled
),

final AS (
    SELECT
        -------- pk
        month,

        -------- metrics
        amortitzation,
        interest,
        debt,
        debt * _personal_percent AS debt_personal,
        home_value,
        home_value * _personal_percent AS home_value_personal,
        equity,
        equity * _personal_percent AS equity_personal,

        -------- metadata
        _source,
        _exported_at,
        _n_updates
    FROM equity
)

SELECT * FROM final
