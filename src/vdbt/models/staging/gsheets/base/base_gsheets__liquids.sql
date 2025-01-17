{%- set accounts = var("expensor")["accounts"]["liquid"] -%}

WITH source AS (
    SELECT *
    FROM {{ source('raw__gsheets', 'liquid_month') }}
),

rename_cols AS (
    SELECT
        *,
        "CC - Caixa enginyers" AS caixa_enginers,
        "Diposit - Caixa enginyers" AS caixa_enginers_diposit,
        "C. sin nómina - ING" AS ing_sin_nomina,
        "C. nómina - ING" AS ing_nomina,
        "C. Naranja - ING" AS ing_naranja,
        "Diposit - ING" AS ing_diposit,
        MyInvestor AS my_investor,
        NB AS norwegian_bank
    FROM source
),

casted_and_renamed AS (
    SELECT
        -------- pk
        replace(Date, '/', '-').concat('-01') :: date AS change_date,

        -------- metrics
        {{ gsheet_to_numeric('total') }} :: decimal(10, 2) AS total,
        {% for col in accounts -%}
            {{ gsheet_to_numeric(col) }} :: decimal(10, 2) AS {{ col }},
        {% endfor -%}

        -------- metadata
        _source,
        _exported_at,
        _n_updates
    FROM rename_cols
)

SELECT *
FROM casted_and_renamed
