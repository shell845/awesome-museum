#
# Functions to create, drop and modify Redshift tables
#
def drop_tables(cur, conn, queries):
    '''
    drop existings redshift table 
    '''
    for query in queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn, queries):
    '''
    create redshift tables
    '''
    for query in queries:
        cur.execute(query)
        conn.commit()