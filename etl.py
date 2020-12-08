import configparser
import os
import psycopg2
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
MUSEUM_DATA_RAW = config['S3']['MUSEUM_DATA']      # csv file
MUSEUM_DATA = config['S3']['MUSEUM_DATA_OUTPUT']   # parquet file output by Spark
WEATHER_DATA_RAW = config['S3']['WEATHER_DATA']    # csv file
WEATHER_DATA = config['S3']['WEATHER_DATA_OUTPUT'] # parquet file output by Spark
CATEGORY_DATA = config['S3']['CATEGORY_DATA']      # json file
RATING_DATA = config['S3']['RATING_DATA']          # json file
TRAVELER_DATA = config['S3']['TRAVELER_DATA']      # json file
CATEGORY_JSONPATH = config['S3']['CATEGORY_JSONPATH']
TRAVELER_JSONPATH = config['S3']['TRAVELER_JSONPATH']

# Country and weather date
COUNTRY = config['FILTER']['COUNTRY']
WEATHER_SINCE = config['FILTER']['DATE']


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
    print("Creat Spark session start")

    conn = psycopg2.connect(f"host={rs_host} dbname={rs_dbname} user={rs_user} password={rs_password} port={rs_port}")
    cur = conn.cursor()

    print("Create Spark session complete")
    return conn, cur


def main():
    """
        Main function to:
            1. Load museum and weather data from S3 for Spark process, and load back to S3
            2. Load museum (processed by spark), weather (processed by spark), category, city and travel data from S3 to Redshift staging
            3. Check data quality in staging
            4. Transform staging data to fact and dimension tables
            3. Check data quality in tables
    """

    print("awesome-museum ETL executing...")

    # step 1
    spark = create_spark_session()

    process_museum_data(spark, MUSEUM_DATA_RAW, MUSEUM_DATA)
    process_weather_data(spark, WEATHER_DATA_RAW, WEATHER_DATA)

    # step 2
    conn, cur = create_redshift_connection()

    drop_tables(conn, cur, drop_table_queries)
    create_tables(conn, cur, create_table_queries)

    staging_museum_sql = staging_museum_table_copy.format(s3_bucket=MUSEUM_DATA, arn_role=IAM)
    staging_museum_data(cur, conn, staging_museum_sql)

    staging_weather_sql = staging_weather_table_copy.format(s3_bucket=WEATHER_DATA, arn_role=IAM)
    staging_weather_data(cur, conn, staging_weather_sql)
                            
    staging_category__sql = staging_category_table_copy.format(s3_bucket=CATEGORY_DATA, arn_role=IAM, json_path=CATEGORY_JSONPATH)
    staging_category_data(cur, conn, staging_category__sql)

    staging_traveler__sql = staging_category_table_copy.format(s3_bucket=TRAVELER_DATA, arn_role=IAM, json_path=TRAVELER_JSONPATH)
    staging_traveler_data(cur, conn, staging_traveler__sql)
    staging_rating_data()
    

    # step 3
    data_quality_check(cur, conn, select_count_staging_queries)

    # step 4
    transform_category()
    transform_traveler()
    transform_rating()
    transform_weather()
    transform_museum()
    transform_museum_fact()

    # step 5
    data_quality_check(cur, conn, select_count_queries)

    # close spark session and redshift connection
    conn.close()
    spark.stop()

    print("awesome-museum ETL complete")


if __name__ == "__main__":
    main()
