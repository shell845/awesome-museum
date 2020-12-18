import configparser

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import DataWranglingOperator, DataStagingOperator, DataQualityCheckOperator, DataTransformationOperator

from helpers import SqlQueries


# read configuration files
config = configparser.ConfigParser()
config.read('/home/workspace/airflow/dags/aws.cfg')

# S3
MUSEUM_DATA_RAW = config['S3']['MUSEUM_DATA_RAW']             # csv file
MUSEUM_DATA = config['S3']['MUSEUM_DATA_OUTPUT']          # parquet file output by Spark
MUSEUM_DATA_S3 = config['S3']['MUSEUM_DATA']  

WEATHER_DATA_RAW = config['S3']['WEATHER_DATA']           # csv file
WEATHER_DATA = config['S3']['WEATHER_DATA_OUTPUT']        # parquet file output by Spark
WEATHER_DATA_S3 = config['S3']['WEATHER_DATA_S3']

CATEGORY_BUCKET = config['S3']['CATEGORY_BUCKET'] 
CATEGORY_KEY = config['S3']['CATEGORY_KEY']
CATEGORY_OUTPUT_KEY = config['S3']['CATEGORY_OUTPUT_KEY']
CATEGORY_DATA = config['S3']['CATEGORY_DATA']             # json file
CATEGORY_DATA_S3 = config['S3']['CATEGORY_DATA_S3']

TRAVELER_BUCKET = config['S3']['TRAVELER_BUCKET'] 
TRAVELER_KEY = config['S3']['TRAVELER_KEY']
TRAVELER_OUTPUT_KEY = config['S3']['TRAVELER_OUTPUT_KEY']
TRAVELER_DATA = config['S3']['TRAVELER_DATA']             # json file
TRAVELER_DATA_S3 = config['S3']['TRAVELER_DATA_S3']

S3_REGION = config['S3']['REGION']

# Country and weather date
COUNTRY = config['FILTER']['COUNTRY']
WEATHER_DATE = config['FILTER']['DATE']


# define DAG
default_args = {
    'owner': 'shell845',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'catchup': False
}

dag = DAG('awesome_museum',
          default_args=default_args,
          description='data pipeline for museum data from TripAdvisor and Kaggle data',
          schedule_interval=None
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

data_wrangling_task = DataWranglingOperator(
    task_id="data_wrangling",
    dag=dag,
    aws_credentials='aws_credentials',
    museum_data_raw=MUSEUM_DATA_RAW,
    museum_data=MUSEUM_DATA,
    weather_data_raw=WEATHER_DATA_RAW,
    weather_data=WEATHER_DATA,
    country=COUNTRY,
    weather_date=WEATHER_DATE,
    category_bucket=CATEGORY_BUCKET,
    category_key=CATEGORY_KEY,
    category_output_key=CATEGORY_OUTPUT_KEY,
    traveler_bucket=TRAVELER_BUCKET,
    traveler_key=TRAVELER_KEY,
    traveler_output_key=TRAVELER_OUTPUT_KEY,
    s3_region=S3_REGION
)


data_staging_task = DataStagingOperator(
    task_id="data_staging",
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials='aws_credentials',
    country=COUNTRY,
    weather_date=WEATHER_DATE,
    museum_data_s3=MUSEUM_DATA_S3,
    weather_data_s3=WEATHER_DATA_S3, 
    category_data_s3=CATEGORY_DATA_S3, 
    traveler_data_s3=TRAVELER_DATA_S3,
    s3_region=S3_REGION
)

staging_data_quality_task = DataQualityCheckOperator(
    task_id='staging_data_quality_check',
    dag=dag,
    redshift_conn_id='redshift',
    count_queries=SqlQueries.select_count_staging_queries,
    show_queries=SqlQueries.check_staging_data_queries
)

data_transformation_task = DataTransformationOperator(
    task_id='data_transformation',
    dag=dag,
    redshift_conn_id='redshift',
    country=COUNTRY,
    weather_date=WEATHER_DATE
)

data_quality_task = DataQualityCheckOperator(
    task_id='data_quality_check',
    dag=dag,
    redshift_conn_id='redshift',
    count_queries=SqlQueries.select_count_queries,
    show_queries=SqlQueries.check_data_queries
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> data_wrangling_task >> data_staging_task >> staging_data_quality_task >> data_transformation_task >> data_quality_task >> end_operator
