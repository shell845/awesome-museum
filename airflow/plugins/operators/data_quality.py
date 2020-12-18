from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.hooks.postgres_hook import PostgresHook

from helpers import SqlQueries


class DataQualityCheckOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 count_queries="",
                 show_queries="",
                 *args,
                 **kwargs):

        super(DataQualityCheckOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.count_queries = count_queries
        self.show_queries = show_queries

    def execute(self, context):
        '''
        Data quality check for Redshift tables
            
            Parameters:
                1) redshift_conn_id: redshift cluster connection
                2) queries: list of sql queries to be executed
        '''

        self.log.info('DataQualityCheckOperator - start')
        
        redshift = PostgresHook(self.redshift_conn_id)
        conn = redshift.get_conn()
        cursor = conn.cursor()
        
        for query in self.count_queries:
            cursor.execute(query)
            results = cursor.fetchone()
            for row in results:
                self.log.info(f'{row} records') 
        
        for query in self.show_queries:
            self.log.info(query) 
            cursor.execute(query)
            results = cursor.fetchall()
            for row in results:
                self.log.info(row) 
         
        self.log.info('DataQualityCheckOperator - complete')