from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.hooks.postgres_hook import PostgresHook

from helpers import SqlQueries

class DataTransformationOperator(BaseOperator):
    ui_color = '#339933'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 country='',
                 weather_date='',
                 *args,
                 **kwargs):
        
        super(DataTransformationOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.country = country
        self.weather_date = weather_date
 

    def execute(self, context):
        '''
        Data transformation for fact and dimension tables
            
            Parameters:
                1) redshift_conn_id: redshift cluster connection
                2) queries: list of sql queries to be executed
        '''
        self.log.info('DataTransformationOperator - start')
        
#         redshift = PostgresHook(self.redshift_conn_id)
        
#         # transform category table
#         redshift.run(SqlQueries.category_table_insert)
        
#         # transform traveler table
#         self.transform_traveler(redshift)
        
#         # transform city table
#         self.transform_city(redshift, SqlQueries.city_table_insert, self.country)
        
#         # transform weather table
#         self.transform_weather(redshift, SqlQueries.weather_table_insert, self.weather_date)
        
#         # transform museum table
#         redshift.run(SqlQueries.museum_table_insert)
        
#         # transform museum fact table
#         self.transform_museum_fact(redshift, SqlQueries.museum_fact_table_insert, self.weather_date)
        
        self.log.info('DataTransformationOperator - complete')
 

    def transform_traveler(self, redshift):
        '''
        Transform traveler data to traveler table
    
        Parameters: self explained
        '''
        traveler_type = ["families", "couples", "solo", "business", "friends"]
        query = """
                insert into traveler (type)
                values ('{}')
                """
        for t in traveler_type:
            sql_stmt = query.format(t)
            redshift.run(sql_stmt)

        print("Complete traveler table")
        
    def transform_city(self, redshift, query, country):
        '''
        Transform traveler data to traveler table
    
        Parameters:
            redshift: redshift connection
            query: sql query
            country: city of which country
        '''
        query = query.format(country)
        redshift.run(query)

        print("Complete city table")
    
    def transform_weather(self, redshift, weather_table_insert, weather_date):
        '''
        Transform weather data to weather table
    
        Parameters:
            redshift: redshift connection
            weather_table_insert: sql query
            weather_date: weather date
        '''
        query = weather_table_insert.format(weather_date)
        redshift.run(query)

        print("Complete weather table")
        
    def transform_museum_fact(self, redshift, museum_fact_table_insert, weather_date):
        '''
        Transform museum data to museum fact table
    
        Parameters:
            redshift: redshift connection
            query: sql query
            weather_date: weather date
        '''
        query = museum_fact_table_insert.format(weather_date)
        redshift.run(query)
        
        print("Complete museum_fact table")
    