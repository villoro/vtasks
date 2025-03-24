{% set min_date = '2025-01-01' %}

WITH source AS (
    SELECT *
    FROM {{ ref('marts_gcal__daily_stats') }}
    WHERE NOT is_personal AND start_day > DATE '{{ min_date }}'
),

final AS (
    SELECT
        start_day,
        duration_hours,
        DATE_TRUNC('week', start_day) AS week,
        STRFTIME(DATE_TRUNC('month', start_day), '%Y-%m') AS month
    FROM source
    ORDER BY 1
)

SELECT * FROM final
