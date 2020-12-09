import configparser

#
# DROP STAGING TABLES
#
staging_category_table_drop = "DROP TABLE IF EXISTS staging_category"
staging_traveler_table_drop = "DROP TABLE IF EXISTS staging_traveler"
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
        category text
    )
""")

staging_traveler_table_create = ("""
    CREATE TABLE staging_traveler (
        museum text,
        families integer,
        couples integer,
        solo integer,
        business integer,
        friends integer
    )
""")


staging_weather_table_create = ("""
    CREATE TABLE staging_weather (
        dt date,
        averageTemperature FLOAT,
        city text,
        country text
    )
""")

staging_museum_table_create = ("""
    CREATE TABLE staging_museum (
        museumname text,
        rating FLOAT,
        city text,
        address text
    )
""")

#
# CREATE FACT AND DIMENSION TABLES
#
city_table_create = ("""
    CREATE TABLE "city" (
        "city_id" IDENTITY(1, 1) PRIMARY KEY,
        "city_name" text NOT NULL SORTKEY DISTKEY,
        "country" text NOT NULL
    )
""")

category_table_create = ("""
    CREATE TABLE "category" (
        "category_id" IDENTITY(1, 1) PRIMARY KEY,
        "category" text NOT NULL
    )
""")

traveler_table_create = ("""
    CREATE TABLE "traveler" (
        "type_id" IDENTITY(1, 1) PRIMARY KEY,
        "type" text NOT NULL
    )
""")

weather_table_create = ("""
    CREATE TABLE "weather" (
        "city_id" integer DISTKEY,
        "weather_date" date SORTKEY,
        "weather" FLOAT
    )
""")

museum_table_create = ("""
    CREATE TABLE "museum" (
        "museum_id" IDENTITY(1, 1) PRIMARY KEY,
        "museum_name" text NOT NULL SORTKEY,
        "category_id" integer NOT NULL,
        "full_address" text NOT NULL,
        "city_id" integer NOT NULL DISTKEY,
        "rating" FLOAT,
    )
""")

museum_fact_table_create = ("""
    CREATE TABLE "museum_fact" (
        "fact_id" IDENTITY(1, 1) PRIMARY KEY,
        "museum_id" integer NOT NULL SORTKEY,
        "category_id" integer NOT NULL,
        "city_id" integer NOT NULL DISTKEY,
        "rating" FLOAT,
        "weather" FLOAT,
        "traveler_type_id" integer
    )
""")


#
# COPY DATA TO STAGING TABLES
#
staging_parquet_copy = ("""
    COPY {table_name}
    FROM {s3_bucket}
    IAM_ROLE {arn_role}
    FORMAT AS PARQUET;
""")

staging_json_copy = ("""
    copy {table_name} 
    from {s3_bucket}
    iam_role {arn_role}
    json 'auto ignorecase'
""")


#
# INSERT DATA TO FACT AND DIMENSION TABLES
#
city_table_insert = ("""
    INSERT INTO city (city_name, country)
    {}
""")

category_table_insert = ("""
    INSERT INTO category (category)
    {}
""")

traveler_table_insert = ("""
    {}
""")

weather_table_insert = ("""
    INSERT INTO weather (city_id, weather_date, weather)
    SELECT c.city_id, w.dt, w.weather
    FROM staging_weather w
    JOIN city c
    ON w.city == c.city
    WHERE w.dt == {}
""")

museum_table_insert = ("""
    INSERT INTO museum (museum_name, category_id, full_address, city_id, rating)
    SELECT sm.museumname, ca.category_id, sm.address, ci.city_id, sm.rating
    FROM staging_museum sm
    JOIN staging_category sc
    ON sm.museumname = sc.museum
    JOIN category ca
    ON sc.category = ca.category
    JOIN city ci
    ON sm.city = ci.city
""")

museum_fact_table_insert = ("""
    INSERT INTO museum_fact (museum_id, category_id, city_id, rating, weather, traveler_type_id)
    SELECT m.museum_id, m.category_id, m.city_id, m.rating, w.weather, 
    FROM museum m
    JOIN weather w
    ON m.city_id = w.city_id
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
create_table_queries = [staging_category_table_create, staging_traveler_table_create, staging_weather_table_create, staging_museum_table_create, city_table_create, category_table_create, traveler_table_create, weather_table_create, museum_table_create, museum_fact_table_create]
drop_table_queries = [staging_category_table_drop, staging_traveler_table_drop, staging_weather_table_drop, staging_museum_table_drop, city_table_drop, category_table_drop, traveler_table_drop, weather_table_drop, museum_table_drop, museum_fact_table_drop]
# copy_table_queries = [staging_weather_table_copy, staging_museum_table_copy, staging_category_table_copy, staging_traveler_table_copy]
insert_table_queries = [city_table_insert, category_table_insert, traveler_table_insert, weather_table_insert, museum_table_insert, museum_fact_table_insert]
select_count_staging_queries= [get_number_staging_category_table, get_number_staging_traveler_table, get_number_staging_weather_table, get_number_staging_museum_table]
select_count_queries= [get_number_city_table, get_number_category_table, get_number_traveler_table, get_number_weather_table, get_number_museum_table, get_number_museum_fact_table]
