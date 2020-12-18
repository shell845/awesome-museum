import configparser

class SqlQueries:

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
            type text,
            number integer
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
            "city_id" INT IDENTITY(1, 1) PRIMARY KEY,
            "city_name" text NOT NULL SORTKEY DISTKEY,
            "country" text NOT NULL
        )
    """)

    category_table_create = ("""
        CREATE TABLE "category" (
            "category_id" INT IDENTITY(1, 1) PRIMARY KEY,
            "category" text NOT NULL
        )
    """)

    traveler_table_create = ("""
        CREATE TABLE "traveler" (
            "type_id" INT IDENTITY(1, 1) PRIMARY KEY,
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
            "museum_id" INT IDENTITY(1, 1) PRIMARY KEY,
            "museum_name" text NOT NULL SORTKEY,
            "category_id" integer NOT NULL,
            "full_address" text NOT NULL,
            "city_id" integer NOT NULL DISTKEY,
            "rating" FLOAT,
            "traveler" text
        )
    """)

    museum_fact_table_create = ("""
        CREATE TABLE "museum_fact" (
            "fact_id" INT IDENTITY(1, 1) PRIMARY KEY,
            "museum_id" integer NOT NULL SORTKEY,
            "category_id" integer NOT NULL,
            "city_id" integer NOT NULL DISTKEY,
            "rating" FLOAT,
            "weather" FLOAT,
            "traveler_type_id" integer,
            "date" date
        )
    """)


    #
    # COPY DATA TO STAGING TABLES
    #
    staging_parquet_copy = ("""
        COPY {table_name}
        FROM '{s3_bucket}'
        IAM_ROLE '{arn_role}'
        FORMAT AS PARQUET;
    """)

    staging_json_copy = ("""
        copy {table_name} 
        from '{s3_bucket}'
        iam_role '{arn_role}'
        json 'auto ignorecase'
    """)


    #
    # INSERT DATA TO FACT AND DIMENSION TABLES
    #
    city_table_insert = ("""
        INSERT INTO city (city_name, country)
        SELECT DISTINCT s.city, '{}' as country
        FROM staging_museum s
        GROUP BY s.city
    """)

    category_table_insert = ("""
        INSERT INTO category (category)
        SELECT DISTINCT s.category
        FROM staging_category s
        GROUP BY s.category
    """)

    traveler_table_insert = ("""
        INSERT INTO traveler (type)
        SELECT DISTINCT type
        FROM staging_traveler
        GROUP BY type
    """)


    weather_table_insert = ("""
        INSERT INTO weather (city_id, weather_date, weather)
        SELECT c.city_id, w.dt, w.averageTemperature
        FROM staging_weather w
        JOIN city c
        ON w.city = c.city_name
        WHERE w.dt = '{}'
    """)

    museum_table_insert = ("""
        INSERT INTO museum (museum_name, category_id, full_address, city_id, rating, traveler)
        SELECT sm.museumname, ca.category_id, sm.address, ci.city_id, sm.rating, t.traveler
        FROM staging_museum sm
        JOIN staging_category sc
        ON sm.museumname = sc.museum
        JOIN category ca
        ON sc.category = ca.category
        JOIN city ci
        ON sm.city = ci.city_name
        LEFT JOIN (SELECT st.museum, st.type AS traveler
                   FROM staging_traveler st
                   INNER JOIN (SELECT t.museum, MAX(t.number) AS max
                               FROM staging_traveler t
                               GROUP BY t.museum) temp
                   ON st.museum = temp.museum
                   AND st.number = temp.max) t
        ON sm.museumname = t.museum
    """)

    museum_fact_table_insert = ("""
        INSERT INTO museum_fact (museum_id, category_id, city_id, rating, traveler_type_id, weather, date)
        SELECT m.museum_id, m.category_id, m.city_id, m.rating, t.type_id, w.weather, '{}'
        FROM museum m
        LEFT JOIN weather w
        ON m.city_id = w.city_id
        LEFT JOIN traveler t
        ON m.traveler = t.type
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
    # SAMPLE CHECK DATA IN STAGING TABLE
    #
    check_data_staging_category = ("""
        SELECT * FROM staging_category LIMIT 5
    """)

    check_data_staging_traveler = ("""
        SELECT * FROM staging_traveler LIMIT 5
    """)

    check_data_staging_weather = ("""
        SELECT * FROM staging_weather LIMIT 5
    """)

    check_data_staging_museum = ("""
        SELECT * FROM staging_museum LIMIT 5
    """)


    #
    # SAMPLE CHECK DATA IN FACT AND DIMENSION TABLE
    #
    check_data_city_table = ("""
        SELECT * FROM city LIMIT 5
    """)

    check_data_category_table = ("""
        SELECT * FROM category LIMIT 5
    """)

    check_data_traveler_table = ("""
        SELECT * FROM traveler  LIMIT 5
    """)

    check_data_weather_table = ("""
        SELECT * FROM weather  LIMIT 5
    """)

    check_data_museum_table = ("""
        SELECT * FROM museum  LIMIT 5
    """)

    check_data_museum_fact_table = ("""
        SELECT * FROM museum_fact  LIMIT 5
    """)

    #
    # CHECK IF ANY NULL VALUE IN FACT TABLE
    #
    check_null =("""
            select *
            from museum_fact
            where museum_id is null 
            or category_id is null 
            or city_id is null 
            or rating is null 
            or weather is null 
            or traveler_type_id is null
            or date is null
    """) 

    #
    # QUERY LISTS
    #
    create_table_queries = [staging_category_table_create, staging_traveler_table_create, staging_weather_table_create, staging_museum_table_create, city_table_create, category_table_create, traveler_table_create, weather_table_create, museum_table_create, museum_fact_table_create]
    drop_table_queries = [staging_category_table_drop, staging_traveler_table_drop, staging_weather_table_drop, staging_museum_table_drop, city_table_drop, category_table_drop, traveler_table_drop, weather_table_drop, museum_table_drop, museum_fact_table_drop]

    insert_table_queries = [city_table_insert, category_table_insert, traveler_table_insert, weather_table_insert, museum_table_insert, museum_fact_table_insert]
    select_count_staging_queries= [get_number_staging_category_table, get_number_staging_traveler_table, get_number_staging_weather_table, get_number_staging_museum_table]
    select_count_queries= [get_number_city_table, get_number_category_table, get_number_traveler_table, get_number_weather_table, get_number_museum_table, get_number_museum_fact_table]

    check_staging_data_queries= [check_data_staging_category, check_data_staging_traveler, check_data_staging_weather, check_data_staging_museum]
    check_data_queries= [check_data_city_table, check_data_category_table, check_data_traveler_table, check_data_weather_table, check_data_museum_table, check_data_museum_fact_table]