from airflow.models import Variable
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.hooks.postgres_hook import PostgresHook

from helpers import SqlQueries


class DataStagingOperator(BaseOperator):
    ui_color = '#9966ff'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 country='',
                 weather_date='',
                 museum_data_s3='',
                 weather_data_s3='',
                 category_data_s3='',
                 traveler_data_s3='',
                 s3_region='',
                 *args,
                 **kwargs):
        super(DataStagingOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.country = country
        self.weather_date = weather_date
        self.museum_data_s3 = museum_data_s3
        self.weather_data_s3 = weather_data_s3
        self.category_data_s3 = category_data_s3
        self.traveler_data_s3 = traveler_data_s3
        self.s3_region = s3_region

    
    def execute(self, context):
        '''
        Data staging
            
        Parameters: self explained
        '''
        
        self.log.info('DataStagingOperator - start copy data to staging')
        
#         # Get AWS variable and configuration
#         arn = Variable.get("arn")

#         # Create Redshift connection
#         redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)        
        
#         # Create tables in Redshift
#         redshift.run(SqlQueries.drop_table_queries)
#         redshift.run(SqlQueries.create_table_queries)

#         # Load data to staging tables
#         staging_museum_sql = SqlQueries.staging_parquet_copy.format(table_name='staging_museum', s3_bucket=self.museum_data_s3, arn_role=arn)
#         redshift.run(staging_museum_sql)
        
#         staging_weather_sql = SqlQueries.staging_parquet_copy.format(table_name='staging_weather', s3_bucket=self.weather_data_s3, arn_role=arn)
#         redshift.run(staging_weather_sql)
    
#         staging_category_sql = SqlQueries.staging_json_copy.format(table_name='staging_category', s3_bucket=self.category_data_s3, arn_role=arn)
#         redshift.run(staging_category_sql) 

#         staging_traveler_sql = SqlQueries.staging_json_copy.format(table_name='staging_traveler', s3_bucket=self.traveler_data_s3, arn_role=arn)
#         redshift.run(staging_traveler_sql)
        
        self.log.info('DataStagingOperator - complete copy data to staging')
        
        