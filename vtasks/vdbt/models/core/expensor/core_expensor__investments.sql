WITH investments AS (
    SELECT * FROM {{ ref('int_expensor__investments') }}
),

home AS (
    SELECT * FROM {{ ref('int_expensor__home_stats') }}
),

final AS (
    SELECT * FROM investments
    UNION ALL BY NAME
    SELECT * FROM home
)

SELECT * FROM final
