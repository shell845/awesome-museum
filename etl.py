import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import functions as F

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
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .getOrCreate()
    return spark


def create_redshift_connection():
    """
        Create Redshift connection
        and return the connection
    """
    conn = psycopg2.connect(f"host={rs_host} dbname={rs_dbname} user={rs_user} password={rs_password} port={rs_port}")
    cur = conn.cursor()
    return conn, cur


def process_song_data(spark, input_data, output_data):
    """
        This function extract song data files from S3,
        transform to songs and artists tables, 
        output as parquet files and load back to S3
        
        Parameters:
            spark: spark session
            input_data: S3 path of input log files
            output_data: S3 path of output parquet files
    """
    
    # song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    # for testing
    # remove for submission
    song_data = input_data + 'song_data/A/D/O/*.json'
    
    
    songSchema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", IntegerType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("year", IntegerType())
    ])
    
    df = spark.read.json(song_data, schema=songSchema)
   
    # create songs table
    song_fields = ["song_id", "title", "artist_id", "year", "duration"]
    songs_table = df.select(song_fields).dropDuplicates()
    
    # write songs table
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/')

    # create artists table
    artists_fields = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude"]
    artists_table = df.selectExpr(artists_fields).dropDuplicates()
    
    # write artists table
    artists_table.write.parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    """
        This function extract log data files from S3,
        transform to songplays, users and time tables, 
        output as parquet files and load back to S3
        
        Parameters:
            spark: spark session
            input_data: S3 path of input log files
            output_data: S3 path of output parquet files
    """
    
    # log data file
    log_data = input_data + 'log_data/*.json'
    # for testing
    # remove for submission
    log_data = input_data + 'log_data/2018/11/2018-11-27-events.json'


    df = spark.read.json(log_data)
    dfNextSong = df.filter(df.page == 'NextSong')

    # create users table    
    dfNextSong = dfNextSong.withColumn("userId", dfNextSong["userId"].cast(IntegerType()))
    users_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = dfNextSong.selectExpr(users_fields).dropDuplicates()
    
    # write users table
    users_table.write.parquet(output_data + "users/")

    # create time table
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0), TimestampType())
    dfNextSong = dfNextSong.withColumn("timestamp", get_timestamp('ts'))
    
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000.0), DateType())
    dfNextSong = dfNextSong.withColumn("start_time", get_datetime('ts'))

    time_table = dfNextSong.select("start_time").dropDuplicates() \
        .withColumn("hour", hour(col("start_time"))).withColumn("day", dayofmonth(col("start_time"))) \
        .withColumn("week", weekofyear(col("start_time"))).withColumn("month", month(col("start_time"))) \
        .withColumn("year", year(col("start_time"))).withColumn("weekday", date_format(col("start_time"), 'E'))
    
    # write time table
    time_table.write.partitionBy("year", "month").parquet(output_data + 'time/')

    # create songplays table
    song_df = spark.read.parquet(output_data + 'songs/*/*/*')
    artists_df = spark.read.parquet(output_data + 'artists/*')
    
    songs_logs = dfNextSong.join(song_df, (dfNextSong.song == song_df.title))
    
    artists_songs_logs = songs_logs.join(artists_df, (songs_logs.artist == artists_df.name)).drop(artists_df.location)
    
    songplays = artists_songs_logs.join(time_table, (artists_songs_logs.start_time == time_table.start_time), 'left').drop(artists_songs_logs.start_time)
    
    songplays = songplays.withColumn("songplay_id", F.monotonically_increasing_id())

    songplays_table = songplays.select(
        col("songplay_id"),
        col('start_time'),
        col('userId').alias('user_id'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId'),
        col('location'),
        col('userAgent').alias('user_agent'),
        col('year').alias('year'),
        col('month').alias('month')
    )

    # write songplays table
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/')


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
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    # step 2
    conn, cur = create_redshift_connection()

    # step 3

    # step 4

    # step 5


if __name__ == "__main__":
    main()
