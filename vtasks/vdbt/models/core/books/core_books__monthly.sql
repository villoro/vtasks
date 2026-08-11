WITH books AS (
    SELECT * FROM {{ ref('marts_books__read') }}
    WHERE read_date IS NOT NULL
),

by_month AS (
    SELECT
        date_trunc('month', read_date) AS month_date,
        count(*) AS books,
        sum(num_pages) AS pages,
        -- `total_hours` comes from a left join against the calendar, so it is
        -- null for books with no reading events logged.
        sum(coalesce(total_hours, 0)) AS hours
    FROM books
    GROUP BY ALL
),

-- Long format, one row per metric, so a single grid and a single smoothing
-- pass cover all three.
long AS (
    UNPIVOT by_month
    ON books, pages, hours
    INTO NAME metric VALUE value
),

-- Fills the months with no book finished at all, which are common and would
-- otherwise make the months either side of the gap behave as consecutive.
grid AS (
    {{ monthly_grid(
        relation='long',
        date_column='month_date',
        measure='value',
        dims=['metric'],
        fill='zero'
    ) }}
),

final AS (
    SELECT
        -------- dims
        month_date,
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
