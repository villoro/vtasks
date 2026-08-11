WITH books AS (
    SELECT * FROM {{ ref('marts_books__read') }}
    WHERE read_date IS NOT NULL
),

by_month AS (
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

long AS (
    UNPIVOT by_month
    ON books, pages, hours
    INTO NAME metric VALUE value
),

-- Most language-months have no book finished at all, so the grid does more
-- work here than in any other series.
grid AS (
    {{ monthly_grid(
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
        is_filled
    FROM grid
    WHERE month_date <= date_trunc('month', CURRENT_DATE)
    ORDER BY ALL
)

SELECT * FROM final
