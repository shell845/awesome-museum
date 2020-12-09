#
# Functions to create, drop and modify Redshift tables
#
def drop_tables(conn, cur, queries):
    '''
    drop existings redshift table 
    '''
    for query in queries:
        cur.execute(query)
        conn.commit()
    print("Complete drop tables")


def create_tables(conn, cur, queries):
    '''
    create redshift tables
    '''
    for query in queries:
        cur.execute(query)
        conn.commit()
    print("Complete create tables")