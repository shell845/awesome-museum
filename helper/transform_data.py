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


def process_weather_data(spark, input_data, output_data, country, weather_date):
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


def staging_parquet_data(cur, conn, queries):
    '''
    copy data from S3 parquet files to Redshift staging table

    Parameters:
        cur: psycopg2 redshift cursor
        conn: psycopg2 redshift connection
        queries: sql query
    '''
    for query in queries:
        cur.execute(query)
        conn.commit()
    print("Copy parquet data from S3 to Redshift staging complete")


def staging_json_data(cur, conn, queries):
    '''
    copy data from S3 json files to Redshift staging table

    Parameters:
        cur: psycopg2 redshift cursor
        conn: psycopg2 redshift connection
        queries: sql queries
    '''
    for query in queries:
        cur.execute(query)
        conn.commit()
    print("Copy json data from S3 to Redshift staging complete")


def transform_category(cur, conn, query):
    '''
    Transform category data to category table
    
    Parameters:
        cur: psycopg2 redshift cursor
        conn: psycopg2 redshift connection
        query: sql query
    '''
    cur.execute(query)
    conn.commit()
    print("Complete category table")


def transform_traveler(cur, conn):
    '''
    Transform traveler data to traveler table
    
    Parameters:
        cur: psycopg2 redshift cursor
        conn: psycopg2 redshift connection
    '''

    traveler_type = ["families", "couples", "solo", "business", "friends"]
    query = """
            insert into traveler (type)
            values ('{}')
        """
    for t in traveler_type:
        sql_stmt = query.format(t)
        cur.execute(sql_stmt)
        conn.commit()

    print("Complete traveler table")


def transform_city(cur, conn, query, country):
    '''
    Transform traveler data to traveler table
    
    Parameters:
        cur: psycopg2 redshift cursor
        conn: psycopg2 redshift connection
        query: sql query
        country: city of which country
    '''

    query = query.format(country)
    cur.execute(query)
    conn.commit()

    print("Complete city table")
 

def transform_weather(cur, conn, weather_table_insert, weather_date):
    '''
    Transform weather data to weather table
    
    Parameters:
        cur: psycopg2 redshift cursor
        conn: psycopg2 redshift connection
        weather_table_insert: sql query
        weather_date: weather date
    '''

    query = weather_table_insert.format(weather_date)
    cur.execute(query)
    conn.commit()

    print("Complete weather table")
    

def transform_museum(cur, conn, query):
    '''
    Transform museum data to museum table
    
    Parameters:
        cur: psycopg2 redshift cursor
        conn: psycopg2 redshift connection
        query: sql query
    '''

    cur.execute(query)
    conn.commit()

    print("Complete museum table")


def transform_museum_fact(cur, conn, query, weather_date):
    '''
    Transform museum data to museum table
    
    Parameters:
        cur: psycopg2 redshift cursor
        conn: psycopg2 redshift connection
        query: sql query
        weather_date: weather date
    '''

    query = query.format(weather_date)
    cur.execute(query)
    conn.commit()

    print("Complete museum_fact table")


