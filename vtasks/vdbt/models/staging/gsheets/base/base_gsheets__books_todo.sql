WITH source AS (
    SELECT * FROM {{ source('raw__gsheets', 'books_todo') }}
),

casted_and_renamed AS (
    SELECT
        -------- info
        Title AS title,
        Author AS author,
        Source AS recommended_by,
        CASE WHEN "Owned?" = 'b' THEN 'In Biblio'
            WHEN "Owned?" = '1' THEN 'In Calibre'
        ELSE NULL END AS owning_type,

        -------- time related
        Added :: date AS added_date,

        -------- measures
        Pages AS num_pages,

        -------- metadata
        _source,
        _exported_at,
        _n_updates
    FROM source
)

SELECT * FROM casted_and_renamed
