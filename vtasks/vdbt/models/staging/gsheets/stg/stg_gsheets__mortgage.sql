{% set percent = 0.7 %}

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

ownership AS (
    SELECT
        *,
        home_value - debt AS ownership
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
        debt * {{ percent }} AS debt_personal,
        home_value,
        home_value * {{ percent }} AS home_value_personal,
        ownership,
        ownership * {{ percent }} AS ownership_personal,

        -------- metadata
        _source,
        _exported_at,
        _n_updates
    FROM ownership
)

SELECT * FROM final
