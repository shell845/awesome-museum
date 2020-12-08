import configparser

#
# CONFIG
#
config = configparser.ConfigParser()
config.read('aws.cfg')

IAM = config['IAM_ROLE']['ARN']

MUSEUM_DATA = config['S3']['MUSEUM_DATA_OUTPUT']   # parquet file output by Spark
WEATHER_DATA = config['S3']['WEATHER_DATA_OUTPUT'] # parquet file output by Spark
CATEGORY_DATA = config['S3']['CATEGORY_DATA']      # json file
RATING_DATA = config['S3']['RATING_DATA']          # json file
TRAVELER_DATA = config['S3']['TRAVELER_DATA']      # json file
LOG_JSONPATH = config['S3']['']

#
# DROP STAGING TABLES
#
staging_category_table_drop = "DROP TABLE IF EXISTS staging_category"
staging_traveler_table_drop = "DROP TABLE IF EXISTS staging_traveler"
staging_rating_table_drop = "DROP TABLE IF EXISTS staging_rating"
staging_weather_table_drop = "DROP TABLE IF EXISTS staging_weather"
staging_museum_table_drop = "DROP TABLE IF EXISTS staging_museum"

#
# DROP FACT AND DIMENSION TABLES
#
city_table_drop = "DROP TABLE IF EXISTS city"
category_table_drop = "DROP TABLE IF EXISTS category"
traveler_table_drop = "DROP TABLE IF EXISTS traveler"
weather_table_drop = "DROP TABLE IF EXISTS weather"
museum_table_drop = "DROP TABLE IF EXISTS museum"
museum_fact_table_drop = "DROP TABLE IF EXISTS museum_fact"

#
# CREATE STAGING TABLES
#
staging_category_table_create = ("""
    CREATE TABLE staging_category (
        museum text,
        museum_type text
    )
""")

staging_traveler_table_create = ("""
    CREATE TABLE staging_traveler (
        museum text,
        traveler_type text,
        traveler_count, integer
    )
""")

staging_rating_table_create = ("""
    CREATE TABLE staging_rating (
        museum text,
        rank text,
        rank_count integer
    )
""")

staging_weather_table_create = ("""
    CREATE TABLE staging_weather (
        dt date,
        AverageTemperature numeric,
        AverageTemperatureUncertainty numeric,
        City text,
        Country text,
        Latitude numeric,
        Longitude numeric
    )
""")

staging_museum_table_create = ("""
    CREATE TABLE staging_museum (
        MuseumName text,
        Rating numeric,
        City text,
        Address text
    )
""")

#
# CREATE FACT AND DIMENSION TABLES
#
city_table_create = ("""
    CREATE TABLE "city" (
        "city_id" bigserial PRIMARY KEY,
        "city_name" text NOT NULL SORTKEY DISTKEY,
        "country" text NOT NULL
    )
""")

category_table_create = ("""
    CREATE TABLE "category" (
        "category_id" serial PRIMARY KEY,
        "category" text NOT NULL
    )
""")

traveler_table_create = ("""
    CREATE TABLE "traveler" (
        "type_id" serial PRIMARY KEY,
        "type" text NOT NULL
    )
""")

weather_table_create = ("""
    CREATE TABLE "weather" (
        "city_id" integer DISTKEY,
        "weather_date" date SORTKEY,
        "weather" numeric
    )
""")

museum_table_create = ("""
    CREATE TABLE "museum" (
        "museum_id" bigserial PRIMARY KEY,
        "museum_name" text NOT NULL SORTKEY,
        "category_id" integer NOT NULL,
        "full_address" text NOT NULL,
        "city_id" integer NOT NULL DISTKEY
    )
""")

museum_fact_table_create = ("""
    CREATE TABLE "museum_fact" (
        "fact_id" bigserial PRIMARY KEY,
        "museum_id" integer NOT NULL SORTKEY,
        "category_id" integer NOT NULL,
        "city_id" integer NOT NULL DISTKEY,
        "rating" numeric,
        "weather" numeric,
        "traveler_type_id" integer
    )
""")


#
# COPY DATA TO STAGING TABLES
#
staging_weather_table_copy = ("""

""")

staging_museum_table_copy = ("""

""")

staging_category_table_copy = ("""
    copy table 
    from {data_bucket}
    iam_role {role_arn}
    json {log_json_path}
    timeformat as 'epochmillisecs'
""").format(data_bucket=LOG_DATA, role_arn=IAM, log_json_path=LOG_JSONPATH)

staging_traveler_table_copy = ("""
    copy table 
    from {data_bucket}
    iam_role {role_arn}
    json {log_json_path}
    timeformat as 'epochmillisecs'
""").format(data_bucket=LOG_DATA, role_arn=IAM, log_json_path=LOG_JSONPATH)

staging_rating_table_copy = ("""
    copy table 
    from {data_bucket}
    iam_role {role_arn}
    json {log_json_path}
    timeformat as 'epochmillisecs'
""").format(data_bucket=LOG_DATA, role_arn=IAM, log_json_path=LOG_JSONPATH)


#
# INSERT DATA TO FACT AND DIMENSION TABLES
#
city_table_insert = ("""
    INSERT INTO city ()
    SELECT 
    FROM
""")

category_table_insert = ("""
    INSERT INTO category ()
    SELECT 
    FROM
""")

traveler_table_insert = ("""
    INSERT INTO traveler ()
    SELECT 
    FROM
""")

weather_table_insert = ("""
    INSERT INTO weather ()
    SELECT 
    FROM
""")

museum_table_insert = ("""
    INSERT INTO museum ()
    SELECT 
    FROM
""")

museum_fact_table_insert = ("""
    INSERT INTO museum_fact ()
    SELECT 
    FROM
""")


#
# GET NUMBER OF ROWS IN STAGING TABLE
#
get_number_staging_category_table = ("""
    SELECT COUNT(*) FROM staging_category
""")

get_number_staging_traveler_table = ("""
    SELECT COUNT(*) FROM staging_traveler
""")

get_number_staging_rating_table = ("""
    SELECT COUNT(*) FROM staging_rating
""")

get_number_staging_weather_table = ("""
    SELECT COUNT(*) FROM staging_weather
""")

get_number_staging_museum_table = ("""
    SELECT COUNT(*) FROM staging_museum
""")


#
# GET NUMBER OF ROWS IN FACT AND DIMENSION TABLE
#
get_number_city_table = ("""
    SELECT COUNT(*) FROM city
""")

get_number_category_table = ("""
    SELECT COUNT(*) FROM category
""")

get_number_traveler_table = ("""
    SELECT COUNT(*) FROM traveler
""")

get_number_weather_table = ("""
    SELECT COUNT(*) FROM weather
""")

get_number_museum_table = ("""
    SELECT COUNT(*) FROM museum
""")

get_number_museum_fact_table = ("""
    SELECT COUNT(*) FROM museum_fact
""")

#
# QUERY LISTS
#
create_table_queries = [staging_category_table_create, staging_traveler_table_create, staging_rating_table_create, staging_weather_table_create, staging_museum_table_create, city_table_create, category_table_create, traveler_table_create, weather_table_create, museum_table_create, museum_fact_table_create]
drop_table_queries = [staging_category_table_drop, staging_traveler_table_drop, staging_rating_table_drop, staging_weather_table_drop, staging_museum_table_drop, city_table_drop, category_table_drop, traveler_table_drop, weather_table_drop, museum_table_drop, museum_fact_table_drop]
copy_table_queries = [staging_weather_table_copy, staging_museum_table_copy, staging_category_table_copy, staging_traveler_table_copy, staging_rating_table_copy]
insert_table_queries = [city_table_insert, category_table_insert, traveler_table_insert, weather_table_insert, museum_table_insert, museum_fact_table_insert]
select_count_staging_queries= [get_number_staging_category_table, get_number_staging_traveler_table, get_number_staging_rating_table, get_number_staging_weather_table, get_number_staging_museum_table]
select_count_queries= [get_number_city_table, get_number_category_table, get_number_traveler_table, get_number_weather_table, get_number_museum_table, get_number_museum_fact_table]
