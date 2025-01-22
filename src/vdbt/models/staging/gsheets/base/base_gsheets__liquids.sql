{%- set accounts = var("expensor")["accounts"]["liquid"] -%}

WITH source AS (
    SELECT *
    FROM {{ source('raw__gsheets', 'liquid_month') }}
),

rename_cols AS (
    SELECT
        *
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
