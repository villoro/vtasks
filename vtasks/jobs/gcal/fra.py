from vtasks.common.duck import read_query

QUERY = """
SELECT start_day, duration_hours
FROM _marts__gcal.marts_gcal__daily_stats
WHERE not is_personal
ORDER BY 1
"""
QUERY = "SHOW ALL TABLES"


def main():
    df = read_query(QUERY)
    return df


if __name__ == "__main__":
    main()
