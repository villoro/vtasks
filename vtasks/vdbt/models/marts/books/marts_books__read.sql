{% set cal_books = '34_Books' %}

WITH books AS (
    SELECT * FROM {{ ref('stg_gsheets__books_read') }}
),

raw_calendar AS (
    SELECT * FROM {{ ref('stg_gcal__events') }}
    WHERE calendar_name = '{{ cal_books }}'
),

calendar AS (
    SELECT
        lower(trim(title)) AS book,
        EXTRACT('year' FROM started_at) AS read_year,
        round(sum(duration_hours), 2) AS total_hours
    FROM raw_calendar
    GROUP BY 1, 2
),

joined AS (
    SELECT
        -------- info
        books.title,
        books.author,
        books.language,
        books.type,

        -------- time related
        books.read_date,

        -------- measures
        books.num_pages,
        SUM(calendar.total_hours) AS total_hours
    FROM books
    LEFT JOIN calendar
        ON lower(trim(books.title)) = lower(calendar.book)
        AND ABS(EXTRACT('year' FROM books.read_date) - calendar.read_year) < 2
    GROUP BY ALL
),

final AS (
    SELECT
        *,
        round(60 * total_hours / num_pages, 3) AS minutes_per_page
    FROM joined
)

SELECT * FROM final
