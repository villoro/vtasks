WITH source AS (
    SELECT * FROM {{ ref('stg_gsheets__books_read') }}
),

years AS (
    SELECT DISTINCT EXTRACT(YEAR FROM read_date) AS year
    FROM source
),

languages AS (
    SELECT DISTINCT language
    FROM source
),

all_combinations AS (
    SELECT year, language
    FROM years
    CROSS JOIN languages
),

aggregated AS (
    SELECT
        EXTRACT(YEAR FROM read_date) AS year,
        language,
        SUM(num_pages) AS total_pages
    FROM source
    GROUP BY EXTRACT(YEAR FROM read_date), language
),

combined AS (
    SELECT
        -------- dims
        all_combinations.year,
        all_combinations.language,

        -------- measures
        COALESCE(aggregated.total_pages, 0) AS total_pages
    FROM all_combinations
    LEFT JOIN aggregated
    ON all_combinations.year = aggregated.year
        AND all_combinations.language = aggregated.language
    ORDER BY ALL
),

with_cumsum AS (
    SELECT
        *,
        SUM(COALESCE(total_pages, 0)) OVER (PARTITION BY language ORDER BY year)
            AS cum_total_pages
    FROM combined
)

SELECT * FROM with_cumsum
