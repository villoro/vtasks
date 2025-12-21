WITH source AS (
    SELECT * FROM {{ source('raw__gsheets', 'mortgage') }}
),

casted_and_renamed AS (
    SELECT
        -------- pk
        replace(Date, '/', '-').concat('-01') :: date AS month,

        -------- metrics
        {{ gsheet_to_numeric('Amortitzation') }} :: decimal(10, 2) AS amortitzation,
        {{ gsheet_to_numeric('Interest') }} :: decimal(10, 2) AS interest,
        {{ gsheet_to_numeric('Debt') }} :: decimal(10, 2) AS debt,
        {{ gsheet_to_numeric('"Home value"') }} :: decimal(10, 2) AS home_value,

        -------- metadata
        _source,
        _exported_at,
        _n_updates
    FROM source
)

SELECT * FROM casted_and_renamed
