import boto3
import json

#
# Functions to process and transform data
#
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
    print("Process museum data start...")
    
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
    # museum_table.write.partitionBy("City").parquet(output_data) # not able to COPY City column to Redshift if partition by City
    museum_table.write.parquet(output_data)

    print("Process museum data complete")


def process_weather_data(spark, input_data, output_data, country, weather_since):
    """
        This function extract weather data files (csv) from S3,
        transform to weather table, 
        output as parquet files and load back to S3
        
        Parameters:
            spark: spark session
            input_data: S3 path of input data files
            output_data: S3 path of output parquet files
            country: weather of which country
            weather_since: weather since which date
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
    df = df.filter(F.col('Country') == country).filter(F.col('dt') >= weather_since)
    
    weather_fields = ["dt", "AverageTemperature", "City", "Country"]
    weather_table = df.select(weather_fields).dropDuplicates()
    
    # verify dataSchema and data
    # weather_table.printSchema()
    # weather_table.show(30)

    # write to parquet
    # weather_table.write.partitionBy("City").parquet(output_data) # not able to COPY City column to Redshift if partition by City
    weather_table.write.parquet(output_data)

    print("Process weather data complete")


def process_category_data(s3_bucket, s3_key, s3_output_key, s3_region, aws_id, aws_key):
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


def process_traveler_data(s3_bucket, s3_key, s3_output_key, s3_region, aws_id, aws_key):
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
    temp = [
    {"museum": key, "Families": values[0], "Couples": values[1], "Solo": values[2],"Business": values[3],"Friends": values[4]}
    for key, values in json_content.items()
    ]

    # output formatted data
    output_data = "".join([json.dumps(line) for line in temp])
    output_object = s3.Object(s3_bucket, s3_output_key)
    output_object.put(Body=(output_data.encode('UTF-8')))

    # set object permission
    object_acl = s3.ObjectAcl(s3_bucket,s3_output_key)
    object_acl.put(ACL='public-read')

    print("Process traveler data complete")


def staging_museum_data(cur, conn, queries):
    '''
    copy data from S3 parquet files to Redshift staging table
    '''
    for query in queries:
        cur.execute(query)
        conn.commit()
    print("Copy data from S3 to staging_museum complete")


def staging_weather_data(cur, conn, queries):
    '''
    copy data from S3 parquet files to Redshift staging table
    '''
    for query in queries:
        cur.execute(query)
        conn.commit()
    print("Copy data from S3 to staging_weather complete")


def staging_category_data(cur, conn, queries):
    pass

def staging_traveler_data(cur, conn, queries):
    pass

def staging_rating_data(cur, conn, queries):
    pass

def transform_category(cur, conn, queries):
    pass

def transform_traveler(cur, conn, queries):
    pass

def transform_rating(cur, conn, queries):
    pass

def transform_weather(cur, conn, queries):
    pass

def transform_museum(cur, conn, queries):
    pass

def transform_museum_fact(cur, conn, queries):
    pass
