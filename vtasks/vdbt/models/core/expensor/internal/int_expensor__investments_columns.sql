WITH invest_month_table_def AS (
	SELECT
		*,
		'raw__gsheets.invest_month' AS table_name
	FROM pragma_table_info('{{ source('raw__gsheets', 'invest_month') }}')
),

worth_month_table_def AS (
	SELECT
		*,
		'raw__gsheets.worth_month' AS table_name
	FROM pragma_table_info('{{ source('raw__gsheets', 'worth_month') }}')
),

all_tables AS (
	SELECT *
	FROM invest_month_table_def
	UNION ALL BY NAME
	SELECT *
	FROM worth_month_table_def
),

cleaned AS (
	SELECT
		cid AS column_id,
		lower(name) AS column_name,
		type AS column_type,
		table_name,
		lower(name) IN ('date', 'total') AS is_index_column,
		lower(name).substring(1, 1) = '_' AS is_metadata_column
	FROM all_tables
),

with_is_account AS (
	SELECT
		*,
		NOT is_index_column AND NOT is_metadata_column AS is_account_column
	FROM cleaned
)

SELECT * FROM with_is_account
