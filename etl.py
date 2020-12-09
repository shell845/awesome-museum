import configparser
import os
import psycopg2
import boto3
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

from helper import *


# Read configuration files
config = configparser.ConfigParser()
config.read('aws.cfg')

# AWS access key
os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']

# AWS role
IAM = config['IAM_ROLE']['ARN']

# Redshift cluster
rs_host=config['CLUSTER']['HOST']
rs_dbname=config['CLUSTER']['DB_NAME']
rs_user=config['CLUSTER']['DB_USER']
rs_password=config['CLUSTER']['DB_PASSWORD']
rs_port=config['CLUSTER']['DB_PORT']

# S3
MUSEUM_DATA_RAW = config['S3']['MUSEUM_DATA']             # csv file
MUSEUM_DATA = config['S3']['MUSEUM_DATA_OUTPUT']          # parquet file output by Spark
MUSEUM_DATA_S3 = config['S3']['MUSEUM_DATA_S3']  

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


def create_spark_session():
    """
        Create Spark session
        Or get existing spark session
        and return the spark session
    """
    print("Creat Spark session start")

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .getOrCreate()

    print("Create Spark session complete")
    return spark


def create_redshift_connection():
    """
        Create Redshift connection
        and return the connection
    """
    print("Connect Redshift start")

    conn = psycopg2.connect(f"host={rs_host} dbname={rs_dbname} user={rs_user} password={rs_password} port={rs_port}")
    cur = conn.cursor()

    print("Connect Redshift complete")
    return conn, cur


def main():
    """
        Main function to:
            1. Load raw data from S3 to Spark for cleaning, formatting and partition, and load back to S3
            2. Load processed (in step 1) data from S3 to Redshift staging
            3. Check data quality in staging
            4. Transform staging data to fact and dimension tables
            3. Check data quality in tables
    """

    print("awesome-museum ETL executing...")

    # step 1
    spark = create_spark_session()

    process_museum_data(spark, MUSEUM_DATA_RAW, MUSEUM_DATA)

    process_weather_data(spark, WEATHER_DATA_RAW, WEATHER_DATA, COUNTRY, WEATHER_DATE)

    process_category_data(CATEGORY_BUCKET, CATEGORY_KEY, CATEGORY_OUTPUT_KEY, S3_REGION, os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])

    process_traveler_data(TRAVELER_BUCKET, TRAVELER_KEY, TRAVELER_OUTPUT_KEY, S3_REGION, os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])


    # step 2
    conn, cur = create_redshift_connection()

    drop_tables(conn, cur, drop_table_queries)
    create_tables(conn, cur, create_table_queries)

    arn = "'" + IAM + "'"

    staging_museum_sql = staging_parquet_copy.format(table_name='staging_museum', s3_bucket=MUSEUM_DATA_S3, arn_role=arn)
    staging_parquet_data(cur, conn, [staging_museum_sql])

    staging_weather_sql = staging_parquet_copy.format(table_name='staging_weather', s3_bucket=WEATHER_DATA_S3, arn_role=arn)
    staging_parquet_data(cur, conn, [staging_weather_sql])
    
    staging_category_sql = staging_json_copy.format(table_name='staging_category', s3_bucket=CATEGORY_DATA_S3, arn_role=arn)
    staging_json_data(cur, conn, [staging_category_sql])   

    staging_traveler_sql = staging_json_copy.format(table_name='staging_traveler', s3_bucket=TRAVELER_DATA_S3, arn_role=arn)
    staging_json_data(cur, conn, [staging_traveler_sql])                   
    

    # step 3
    data_quality_check(cur, conn, select_count_staging_queries)

    # step 4
    transform_category(cur, conn, category_table_insert)
    transform_traveler(cur, conn)
    transform_city(cur, conn, city_table_insert)
    transform_weather(cur, conn, weather_table_insert, WEATHER_DATE)
    transform_museum()
    transform_museum_fact()

    # step 5
    data_quality_check(cur, conn, select_count_queries)

    # close spark session and redshift connection
    cur.close()
    conn.close()
    spark.stop()

    print("awesome-museum ETL complete")


if __name__ == "__main__":
    main()
