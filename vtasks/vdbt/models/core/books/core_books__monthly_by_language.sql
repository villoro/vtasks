WITH books AS (
    SELECT * FROM {{ ref('marts_books__read') }}
    WHERE read_date IS NOT NULL
),

by_language AS (
    SELECT
        date_trunc('month', read_date) AS month_date,
        language,
        count(*) AS books,
        sum(num_pages) AS pages,
        -- `total_hours` comes from a left join against the calendar, so it is
        -- null for books with no reading events logged.
        sum(coalesce(total_hours, 0)) AS hours
    FROM books
    GROUP BY ALL
),

-- Carried as an extra `language` so a chart gets one trace per language plus
-- the total from a single breakout. Smoothed as its own series, which for a
-- linear filter is the same as summing the smoothed languages.
totals AS (
    SELECT
        date_trunc('month', read_date) AS month_date,
        'Total' AS language,
        count(*) AS books,
        sum(num_pages) AS pages,
        sum(coalesce(total_hours, 0)) AS hours
    FROM books
    GROUP BY ALL
),

combined AS (
    SELECT * FROM by_language UNION ALL BY NAME
    SELECT * FROM totals
),

long AS (
    UNPIVOT combined
    ON books, pages, hours
    INTO NAME metric VALUE value
),

-- Most language-months have no book finished at all, so the grid does more
-- work here than in any other series.
grid AS (
    {{ date_grid(
        relation='long',
        date_column='month_date',
        measure='value',
        dims=['language', 'metric'],
        fill='zero'
    ) }}
),

final AS (
    SELECT
        -------- dims
        month_date,
        language,
        metric,

        -------- measures
        round(value, 2) AS value,

        -------- metadata
        language = 'Total' AS is_total,
        is_filled
    FROM grid
    WHERE month_date <= date_trunc('month', CURRENT_DATE)
    ORDER BY ALL
)

SELECT * FROM final
