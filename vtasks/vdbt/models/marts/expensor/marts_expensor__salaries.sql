WITH source AS (
    SELECT *
    FROM {{ ref('stg_gsheets__salaries') }}
),

un_pivoted AS (
    UNPIVOT source
    ON
        gross_fixed_salary, bonus, eags, total_gross_salary, total_net_salary
    INTO
    NAME trace
    VALUE value_eur
),

final AS (
    SELECT
        -------- pks
        change_date,
        trace,

        -------- measures
        value_eur,

        -------- metadata
        _source,
        _exported_at,
        _n_updates
    FROM un_pivoted
)

SELECT *
FROM final
