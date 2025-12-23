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
        *,
        home_value - debt AS equity
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
        IF(YEAR(month) >= {{ buy_year }}, debt * {{ percent }}, debt) AS debt_personal,
        home_value,
        IF(YEAR(month) >= {{ buy_year }}, home_value * {{ percent }}, home_value) AS home_value_personal,
        equity,
        IF(YEAR(month) >= {{ buy_year }}, equity * {{ percent }}, equity) AS equity_personal,

        -------- metadata
        _source,
        _exported_at,
        _n_updates
    FROM equity
)

SELECT * FROM final
