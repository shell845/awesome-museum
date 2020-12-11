## Project summary
This project collects and presents information about museums (US only at current stage, will extend to global in next phase), including museum categories, locations, ratings and more. 

Output can be used by travellers, museum fans and travel companies for trip planning, as well can be used by museum marketing department and data analysts for study and analyse.

## Project structure
This repository contains following materials:

1. **`aws.cfg`** contains all configuration parameters. Please input following parameters before executing the ETL pipeline.

2. **`helper`** folder contains following helper functions:

	- `create_drop_tables.py` drop and create redshift tables
	- `data_quality.py` data quality check
	- `sql_queries.py` all sql queries used in this project
	- `transform_data.py` functions to process data and transform table
	
    
3. **`etl.py`** is the ETL pipleline

4. **`small_dataset`** datasets in smaller size for testing purpose

5. **`awesome-museum.ipynb`** is a Jupyter notebook for testing and can be used as dashboard

6. **`Data_Dictionary.md`** is the data dictionary of this project


## Instruction
Please follow below steps to run this project:

**Step 1: Fill in `aws.cfg`**
	
	
	[KEYS]
	AWS_ACCESS_KEY_ID=<your_access_id>
	AWS_SECRET_ACCESS_KEY=<your_access_key>
	
	[CLUSTER]
	HOST=<your_redshift_host>
	DB_NAME=<your_redshift_db_name>
	DB_USER=<your_redshift_db_user>
	DB_PASSWORD=<your_redshift_db_password>
	DB_PORT=5439
	
	[IAM_ROLE]
	ARN=<your_arn>
	
	[FILTER]
	COUNTRY=United States
	DATE=2012-10-01
	

- Notes for [FILTER] section
	- For COUNTRY, we only have dataset for United States at current stage. 
	- For DATE, we only have dataset up to 2013-09-01, every first day of each month.

**Step 2: Remove folder `museum-output` and `weather-output` if they already exist in your S3 output directory.**

**Step 3: Execute `etl.py`**

Note: You may change to use smaller size source dataset in `small_dataset` for testing.

## Source data
Datasets are collected from Kaggle and Tripadvisor and are uploaded to AWS S3 `s3://udacity-dend-shell845/museum-data/`

1. **museum** overall summary of museums extracted from Tripadvisor. In csv format.   
2. **category** categories of museums, e.g. art museum, history museum, science museum etc. In json format.
3. **rating** how many ratings did the museums receive and what are the ratings. In json format.
4. **traveler** how the travers travel. In json format.
5. **weather** average temperature of the cities where the museums are located. In csv format.

Please refer to the [Data Dictionary](Data_Dictionary.md) file for detail.

## Data model

![DB Diagram](DB-diagram.png)

## Data Dictionary

This project has following tables:

1. `staging_category` staging table for category dataset
2. `staging_traveler` staging table for traveler dataset
3. `staging_weather` staging table for weather dataset
4. `staging_museum` staging table for museum dataset
5. `museum_fact` fact table and final product of this project
6. `city` dimension table for city information
7. `category` dimension table for museum's category information
8. `traveler` dimension table for traveler types
9. `weather` dimension table for weather information
10. `museum` dimension table for museum information

Please refer to the [Data Dictionary](Data_Dictionary.md) file for detail.


## ETL pipeline
The ETL pipeline includes following steps:

1. Clean and re-format raw data
	- Load raw data for **museum** and **weather** from S3 to Spark, since these 2 datasets are too big to be processed directly in Redshift.
	- Clean **museum** and **weather** raw data with Spark, output transformed data to S3 as parquet files.
	- Clean and re-format **category** and **traveler** dataset, output re-formatted data to S3 as json files.
2. Load cleaned and well-formatted dataset from S3 to Redshift staging tables.
3. Conduct data quality check to ensure data are loaded successfully to Redshift.
4. Transform staging tables to fact and dimension tables in Redshift.
5. Perform data quality check to ensure data transformation is correct.

![ETL Pipeline](elt-diagram.png)

## Data quality check
Three types of data quality checks are performed in this project:
1. Check record counts of each data to ensure data are properly processed.
2. Select and display sample data to further exam the data quality.
3. Check if any null values in tables.

## Tools and technologies
S3, Spark and Redshift are use in this project.

**S3** provides object-based storage which make it suitable to be the data lake of this project, because there are several types of dataset files being used and generated in this project, including json files, csv files and parquet files. In addition, storing datasets in S3 make them easier to be shared among Spark and Redshift.

**Spark** is ideal to process mass data. Our raw datasets are big, for example there are millions rows of records in the weather raw data file. It will be time consuming if we use Redshift to clean, format and process those big files directly with Redshift. So we use Spark to pre-process the raw datasets, do data cleaning and data wrangling, and write results as parquet files back to S3 for Redshift's further processing.

**Redshift** is used as data warehouse for this project, all staging tables, fact table and dimension tables are host in Redshift. It enables us to pull data from multiple sources (in our case, the multiple sources means multiple data files stored in S3) and transform as well as analyse. Also, Redshift is horizontally scalable, which means we can still easier handle our data when our data volume increases x100+.

Will further adopt Airflow for pipeline management in next phase.

## Usage
Museum lovers can make reference to the project dataset for their trip plannings.

Travel agencies can make use of this project for travel recommendations as well.

Data analysts from travel industry can use this project output to predict if a museum will be popular with market, and study what factors make a museum popular.

Museum staff can get intuitive feedback on how visitors view their museum.


## Other scenarios
1. If the data was increased by 100x.

	Spark will be used to process all raw datasets. Redshift cluster will be expanded.


2. If the pipelines would be run on a daily basis by 7 am every day.

	Airflow will be used for pipelines scheduling.


3. If the database needed to be accessed by 100+ people.

	If those people only need to view the project result, then they should not access the database directly. Instead the result will be visualised by visualization tool e.g Tableau, Power BI etc. Only the visualization tool access database, people only check results from the tool.
	
	If those people need read write to the database, an IAM group will be assigned to them.


## Next phase
1. Adopt Airflow for pipeline management.
2. Visualise the data.
2. Extend the dataset to including reviewer comments, study the relationship between rating and different key words in the comments.

 