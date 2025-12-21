WITH source AS (
    SELECT * FROM {{ source('raw__gsheets', 'salary') }}
),

casted_and_renamed AS (
    SELECT
        -------- pk
        Date :: date AS change_date,

        -------- metrics
        {{ gsheet_to_numeric('Fixed') }} :: decimal(10, 2) AS gross_fixed_salary,
        {{ gsheet_to_numeric('Bonus') }} :: decimal(10, 2) AS bonus,
        {{ gsheet_to_numeric('EAGI') }} :: decimal(10, 2) AS eags,
        {{ gsheet_to_numeric('Total') }} :: decimal(10, 2) AS total_gross_salary,
        {{ gsheet_to_numeric('"Net Y"') }} :: decimal(10, 2) AS total_net_salary,
        Hours AS hours,

        -------- metadata
        _source,
        _exported_at,
        _n_updates
    FROM source
)

SELECT * FROM casted_and_renamed
