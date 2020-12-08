import configparser
import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

from sql_queries import *


# Read configuration files
config = configparser.ConfigParser()
config.read('aws.cfg')

# AWS access key
os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']

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


def process_museum_data(spark, input_data, output_data):
    """
        This function extract museum data files (csv) from S3,
        transform to museum table, 
        output as parquet files and load back to S3
        
        Parameters:
            spark: spark session
            input_data: S3 path of input data files
            output_data: S3 path of output parquet files
    """
    print("Process museum data start")
    
    museum_data = input_data + '*.csv'
 
    museumSchema = StructType([
        StructField("mid", StringType()),
        StructField("Address", StringType()),
        StructField("Description", StringType()),
        StructField("FeatureCount", IntegerType()),
        StructField("Fee", StringType()),
        StructField("Langtitude", DoubleType()),
        StructField("Latitude", DoubleType()),
        StructField("LengthOfVisit", StringType()),
        StructField("MuseumName", StringType()),
        StructField("PhoneNum", StringType()),
        StructField("Rank", DoubleType()),
        StructField("Rating", DoubleType()),
        StructField("ReviewCount", StringType()),
        StructField("TotalThingsToDo", StringType())
    ])

    df = spark.read.format("csv").option("header", True).schema(museumSchema).load(museum_data)
   
    split_address = F.split(df["Address"], ", ")
    df = df.withColumn("City", split_address.getItem(F.size(split_address) - 2))
    museum_fields = ["MuseumName", "Rating", "City", "Address"]
    museum_table = df.select(museum_fields).dropDuplicates()

    # write to parquet
    museum_table.write.partitionBy("City").parquet(output_data)

    print("Process museum data complete")


def process_weather_data(spark, input_data, output_data):
    """
        This function extract weather data files (csv) from S3,
        transform to weather table, 
        output as parquet files and load back to S3
        
        Parameters:
            spark: spark session
            input_data: S3 path of input data files
            output_data: S3 path of output parquet files
    """
    print("Process weather data start")
    weather_data = input_data + '*.csv'
 
    weatherSchema = StructType([
        StructField("dt", DateType()),
        StructField("AverageTemperature", DoubleType()),
        StructField("City", StringType()),
        StructField("Country", StringType())
    ])

    df = spark.read.format("csv").option("header", True).schema(weatherSchema).load(weather_data)

    df = df.filter(df("dt").gt(lit("2013-01-01"))) 
   
    weather_fields = ["dt", "AverageTemperature", "City", "Country"]
    weather_table = df.select(weather_fields).dropDuplicates()

    # write to parquet
    weather_table.write.partitionBy("City").parquet(output_data)

    print("Process weather data complete")


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



def main():
    """
        Main function to:
            1. Load museum and weather data from S3 for Spark process, and load back to S3
            2. Load museum (processed by spark), weather (processed by spark), category, city and travel data from S3 to Redshift staging
            3. Check data quality in staging
            4. Transform staging data to fact and dimension tables
            3. Check data quality in tables
    """

    # step 1
    spark = create_spark_session()

    process_museum_data(spark, MUSEUM_DATA_RAW, MUSEUM_DATA)
    process_weather_data(spark, WEATHER_DATA_RAW, WEATHER_DATA)

    # step 2
    conn, cur = create_redshift_connection()

    drop_tables(conn, cur, drop_table_queries)
    create_tables(conn, cur, create_table_queries)

    staging_category_data()
    staging_traveler_data()
    staging_rating_data()
    staging_museum_data()
    staging_weather_data()

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


if __name__ == "__main__":
    main()
