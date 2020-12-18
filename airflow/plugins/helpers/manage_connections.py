import os

from pyspark.sql import SparkSession
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.postgres_operator import PostgresOperator


class ManageConnections:
    def create_spark_session(self, log, aws_id, aws_key):
        """
            Create Spark session
            Or get existing spark session
            and return the spark session
        """
        
        # Create spark session
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.awsAccessKeyId", aws_id) \
            .config("spark.hadoop.fs.s3a.awsSecretAccessKey", aws_key) \
            .getOrCreate()
        
        log.info("Created Spark session")
        return spark

    
    def close_spark_session(self, log, spark):
        """
            Close spark session
        """
        spark.stop()
        log.info("Closed Spark session")