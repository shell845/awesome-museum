import boto3
import json
import os

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

from helpers import *

class DataWranglingOperator(BaseOperator):
    ui_color = '#80d4ff'
    
    @apply_defaults
    def __init__(self,
                 aws_credentials='',
                 museum_data_raw='',
                 museum_data='',
                 weather_data_raw='',
                 weather_data='',
                 country='',
                 weather_date='',
                 category_bucket='',
                 category_key='',
                 category_output_key='',
                 traveler_bucket='',
                 traveler_key='',
                 traveler_output_key='',
                 s3_region='',
                 *args,
                 **kwargs):
        super(DataWranglingOperator, self).__init__(*args, **kwargs)
        self.aws_credentials=aws_credentials
        self.museum_data_raw=museum_data_raw
        self.museum_data=museum_data
        self.weather_data_raw=weather_data_raw
        self.weather_data=weather_data
        self.country=country
        self.weather_date=weather_date
        self.category_bucket=category_bucket
        self.category_key=category_key
        self.category_output_key=category_output_key
        self.traveler_bucket=traveler_bucket
        self.traveler_key=traveler_key
        self.traveler_output_key=traveler_output_key
        self.s3_region=s3_region

    
    def execute(self, context):
        '''
        Data wrangling
            
        Parameters: self explained
        '''
        
        self.log.info('DataWranglingOperator - start to clean and format raw data')
        
#         # Get AWS configuration
#         aws_hook = AwsHook(self.aws_credentials)
#         credentials = aws_hook.get_credentials()        

#         os.environ['AWS_ACCESS_KEY_ID']=credentials.access_key
#         os.environ['AWS_SECRET_ACCESS_KEY']=credentials.secret_key
        
#         self.log.info('DataWranglingOperator - create spark connection')
#         spark = ManageConnections().create_spark_session(self.log, os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])
        
#         self.log.info('DataWranglingOperator - clean museum data')
#         self.process_museum_data(spark, self.museum_data_raw, self.museum_data)
        
#         self.log.info('DataWranglingOperator - clean weather data')
#         self.process_weather_data(spark, self.weather_data_raw, self.weather_data, self.country, self.weather_date)
        
#         self.log.info('DataWranglingOperator - clean category data')
#         self.process_category_data(self.category_bucket, self.category_key, self.category_output_key, self.s3_region, os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])
        
#         self.log.info('DataWranglingOperator - clean traveler data')
#         self.process_traveler_data(self.traveler_bucket, self.traveler_key, self.traveler_output_key, self.s3_region, os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])
 
#         ManageConnections().close_spark_session(self.log, spark)
    
        self.log.info('DataWranglingOperator - complete clean and format raw data')
        
    
    #
    # Functions to process and transform data
    #
    def process_museum_data(self, spark, input_data, output_data):
        """
            This function extract museum data files (csv) from S3,
            transform to museum table, 
            output as parquet files and load back to S3
            
            Parameters:
                spark: spark session
                input_data: S3 path of input data files
                output_data: S3 path of output parquet files
        """
        self.log.info('DataWranglingOperator - start to process museum data')
        
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
        museum_table = museum_table.na.drop()
        
        # verify dataframe schema and data
        # museum_table.printSchema()
        # museum_table.show(5)

        # write to parquet
        # not able to COPY City column to Redshift if partition by City
        # museum_table.write.partitionBy("City").parquet(output_data) 
        # museum_table.write.parquet(output_data)

        self.log.info('DataWranglingOperator - complete to process museum data')
           

    def process_weather_data(self, spark, input_data, output_data, country, weather_date):
        """
            This function extract weather data files (csv) from S3,
            transform to weather table, 
            output as parquet files and load back to S3
            
            Parameters:
                spark: spark session
                input_data: S3 path of input data files
                output_data: S3 path of output parquet files
                country: weather of which country
                weather_date: weather since which date
        """
        
        print("Process weather data start...")
        weather_data = input_data + '*.csv'
     
        weatherSchema = StructType([
            StructField("dt", DateType()),
            StructField("AverageTemperature", DoubleType()),
            StructField("AverageTemperatureUncertainty", DoubleType()),
            StructField("City", StringType()),
            StructField("Country", StringType()),
            StructField("Latitude", StringType()),
            StructField("Longitude", StringType())
        ])

        df = spark.read.format("csv").option("header", True).schema(weatherSchema).load(weather_data)
        df = df.filter(F.col('Country') == country).filter(F.col('dt') >= weather_date)
        
        weather_fields = ["dt", "AverageTemperature", "City", "Country"]
        weather_table = df.select(weather_fields).dropDuplicates()
        
        # verify dataSchema and data
        # weather_table.printSchema()
        # weather_table.show(30)

        # write to parquet
        # weather_table.write.partitionBy("City").parquet(output_data) # not able to COPY City column to Redshift if partition by City
        # weather_table.write.parquet(output_data)

        print("Process weather data complete")


    def process_category_data(self, s3_bucket, s3_key, s3_output_key, s3_region, aws_id, aws_key):
        """
            This function load category data file (json) from S3,
            do data cleaning and re-format,
            and write back to S3
            
            Parameters:
                s3_bucket: S3 bucket
                s3_key: S3 key for input raw data file
                s3_output_key: S3 key for output file
                s3_region: S3 region
                aws_id: AWS access key id
                aws_key: AWS access key secret
        """
        print("Process category data start")
        # read in raw data
        s3 = boto3.resource('s3',
                        region_name=s3_region,
                        aws_access_key_id=aws_id,
                        aws_secret_access_key=aws_key
                       )
        input_object = s3.Object(s3_bucket, s3_key)
        file_content = input_object.get()['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)

        # format raw data
        temp = [
        {"museum": key, "category": values[0]}
        for key, values in json_content.items()
        ]

        # output formatted data
        output_data = "".join([json.dumps(line) for line in temp])
        output_object = s3.Object(s3_bucket, s3_output_key)
        output_object.put(Body=(output_data.encode('UTF-8')))

        # set object permission
        object_acl = s3.ObjectAcl(s3_bucket,s3_output_key)
        object_acl.put(ACL='public-read')

        print("Process category data complete")


    def process_traveler_data(self, s3_bucket, s3_key, s3_output_key, s3_region, aws_id, aws_key):
        """
            This function load traveler data file (json) from S3,
            do data cleaning and re-format,
            and write back to S3
            
            Parameters:
                s3_bucket: S3 bucket
                s3_key: S3 key for input raw data file
                s3_output_key: S3 key for output file
                s3_region: S3 region
                aws_id: AWS access key id
                aws_key: AWS access key secret
        """
        print("Process traveler data start")
        # read in raw data
        s3 = boto3.resource('s3',
                        region_name=s3_region,
                        aws_access_key_id=aws_id,
                        aws_secret_access_key=aws_key
                       )
        input_object = s3.Object(s3_bucket, s3_key)
        file_content = input_object.get()['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)
        

        # format raw data
        # temp = [
        #     {"museum": key, 'families': int(values[0].replace(",", "")),'couples': int(values[1].replace(",", "")),'solo': int(values[2].replace(",", "")),'business': int(values[3].replace(",", "")),'friends': int(values[4].replace(",", ""))}
        #     for key, values in json_content.items()
        # ] 
        temp = []
        for key, values in json_content.items():
            temp.append({"museum":key, "type":"families", "number":int(values[0].replace(",", ""))})
            temp.append({"museum":key, "type":"couples", "number":int(values[1].replace(",", ""))})
            temp.append({"museum":key, "type":"solo", "number":int(values[2].replace(",", ""))})
            temp.append({"museum":key, "type":"business", "number":int(values[3].replace(",", ""))})
            temp.append({"museum":key, "type":"friends", "number":int(values[4].replace(",", ""))})

        # output formatted data
        output_data = "".join([json.dumps(line) for line in temp])
        output_object = s3.Object(s3_bucket, s3_output_key)
        output_object.put(Body=(output_data.encode('UTF-8')))

        # set object permission
        object_acl = s3.ObjectAcl(s3_bucket,s3_output_key)
        object_acl.put(ACL='public-read')

        print("Process traveler data complete")



