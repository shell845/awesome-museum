#
# Functions for data quality check
#

def data_quality_check(cur, conn, queries):
    """
    get record counts of each table

    parameters:
    1. cur : cursor of Redshift
    2. conn : connection with Redshift
    3. queries: sql queries to be executed
    """
    print(f"Data quality check - {queries}")
    for query in queries:
        print('Running ' + query)
        cur.execute(query)
        results = cur.fetchone()

        for row in results:
            print("   ", row)