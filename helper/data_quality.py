#
# Functions for data quality check
#

def data_quality_check_null(cur, conn, check_null):
    """
    check if any null value in table

    parameters:
    1. cur : cursor of Redshift
    2. conn : connection with Redshift
    """    
    print("Data quality - check if have null value in table")  

    cur.execute(check_null)
    results = cur.fetchall()
    for row in results:
            print(row)
            
    if len(results) > 0: # null value exists
        print("Data quality - NULL value exists in table, please check if any missing data")
    else:
        print("Data quality - check null complete")
       
        
def data_number_check(cur, conn, queries):
    """
    get record counts for each table

    parameters:
    1. cur : cursor of Redshift
    2. conn : connection with Redshift
    3. queries: sql queries to be executed
    """
    print("Data quality - check record count for each table")   
    for query in queries:
        print('Running ' + query)
        cur.execute(query)
        results = cur.fetchone()

        for row in results:
            print("   ", row)            
    print("Data quality - check record count complete")

        
def data_quality_check(cur, conn, queries):
    """
    sample check data in each table

    parameters:
    1. cur : cursor of Redshift
    2. conn : connection with Redshift
    3. queries: sql queries to be executed
    """
    print("Data quality - check data detail for each table")    
    for query in queries:
        print('Running ' + query)
        cur.execute(query)
        results = cur.fetchall()
        for row in results:
            print(row)            
    print("Data quality - check data detail complete")
    

    
