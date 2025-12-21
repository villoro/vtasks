WITH source AS (
    SELECT * FROM {{ source('raw__gsheets', 'share_percentages') }}
),

casted AS (
    SELECT
        *,
        Date :: date AS start_date,
    FROM source
),

renamed AS (
    SELECT
        -------- time related
        start_date,
        LEAD(start_date, 1, DATE '2222-2-2') OVER (ORDER BY start_date) AS end_date,

        -------- metrics
        ROUND({{ gsheet_to_numeric('Percent') }} :: decimal(10, 2) / 100, 4) AS percent,

        -------- metadata
        _source,
        _exported_at,
        _n_updates
    FROM casted
)

SELECT *
FROM renamed
