## Project summary
This project collects and presents information about museums (US only at current stage, may extend to global in next pharse), including museum categories, locations, ratings and more. 

## Project structure
This project reposity contains following:

1. etl.py
2. sql_queries.py
3. aws.cfg

## Source data
Datasets are collected from Kaggle and Tripadvisor and are uploaded to AWS S3 `s3://udacity-dend-shell845/museum-data/`

1. **museum** overall summary of museums extracted from Tripadvisor. In csv format.
	
2. **category** categories of museums, e.g. art museum, history museum, science museum etc. In json format.
	
3. **rating** how many ratings did the museums receive and what are the ratings. In json format.

4. **traveler** how the travers travel. In json format.

5. **weather** average tempature of the cities where the museums are located. In csv format.


## Data model
### Staging table
1. `staging_category` load data from **category**
	
	`{'museum': ['museum type 1','museum type 2', …]}`
	`{"Gettysburg Heritage Center": ["History Museums", "Museums"]}`

2. `staging_rating` load data from **rating**

	`{'museum': ['Excellent','Very good','Average','Poor','Terrible']}`
	`{"Gettysburg Heritage Center": ["164", "63", "10", "5", "4"]}`

3. `staging_traveler` load data from **traveler**

	`{'museum': ['Families','Couples','Solo','Business','Friends']}`
	`{"Gettysburg Heritage Center": ["88", "86", "17", "2", "33"]`

4. `staging_weather` load data from **weather**
	
	`dt,AverageTemperature,AverageTemperatureUncertainty,City,Country,Latitude,Longitude`
	`2008-01-01,10.915999999999999,0.165,Pasadena,United States,29.74N,96.00W`

5. `staging_museum` load data from **museum**
	
	`Address, Description, FeatureCount, Fee, Langtitude, Latitude, LengthOfVisit, MuseumName, PhoneNum, Rank, Rating, ReviewCount,TotalThingsToDo`
	`"945 Magazine Street, New Orleans, LA 70130-3813","Founded by historian and author, Stephen Ambrose, the Museum tells the story of the American Experience in the war that changed the world - why it was fought, how it was won, and what it means today - so that all generations will understand the price of freedom and be inspired by what they learn. ",11,NA,-90.07008599999999,29.9430044,NA,The National WWII Museum,+1 504-528-1944,1,5.0,"15,611",319`

### Fact table
#### museum_fact
| Field Name | Data Type | Description                          | Example                      |
|------------|-----------|--------------------------------------|------------------------------|
| fact_id    | bigserial | primary key, id of the museum fact   | 2904                         |
| museum_id  | integer   | museum id                            | 38                           |
| city_id    | integer   | city id of museum location           | 72                           |
| category_id| integer   | id of museum category                | 2                            |
| rating     | numeric   | average rating given by visitors     | 4.7                          |
| traveler_type_id|integer| id of visitor type                  | 3                            |
| weather    | numeric   | average temperature of the city      | 17.4                         |


### Dimension table
#### city
| Field Name | Data Type | Description                 | Example      |
|------------|-----------|-----------------------------|--------------|
| city_id    | bigserial | primary key, unique city id | 28           |
| city_name  | text      | city name                   | New York     |
| country    | text      | city country                | United State |

#### category
| Field Name  | Data Type | Description                            | Example     |
|-------------|-----------|----------------------------------------|-------------|
| category_id | serial    | primary key, unique id of the category | 29          |
| category    | text      | museum category                        | Arts museum |

#### traveler
| Field Name | Data Type | Description   | Example  |
|------------|-----------|---------------|----------|
| type_id    | serial    | primary key   | 1        |
| type       | text      | traveler type | Families |

#### weather
| Field Name | Data Type | Description                    | Example    |
|------------|-----------|--------------------------------|------------|
| city_id    | integer   | refers to city_id of city      | 1          |
| weather_date| date     | date of weather                | 2019-06-01 |
| weather    | numeric   | average temperature in celsius | 17.8       |

#### museum
| Field Name   | Data Type | Description                   | Example                        |
|--------------|-----------|-------------------------------|--------------------------------|
| museum_id    | bigserial | primary key, unique museum id | 281                            |
| museum_name  | text      | museum name                   | The Metropolitan Museum of Art |
| category_id  | integer   | what type of museum           | 2                              |
| full_address | text      | full address of the museum    | 1000 Fifth Avenue              |
| city_id      | integer   | city id                       | 827                            |


### DB diagram
![DB-diagram](DB-diagram.png)

## ETL pipeline
The ETL pipeline includes following steps:

1. Load raw data for **museum** and **weather** from S3 to Spark, since these 2 datasets are too big to be processed directly in Redshift.
2. Process **museum** and **weather** raw data with Spark, output transformed data to S3 as parquet files.
3. Load processed dataset **museum** and **weather** as well as small raw datasets **category**, **rating** and **traveler** from S3 to Redshift staging tables.
4. Conduct data quality check to ensure data are loaded succesfully to Redshift.
5. Transform staging tables to fact and dimension tables in Redshift.
6. Perform data quality check to ensure data transformation is correct.
7. Analyze data.

## Data quality check

## Tools and technologies

## Usage
Museum lovers can make reference to the project dataset for their trip plannings.

Travel agencies can make use of this project for travel recommendations as well.

Data analysts from travel industry can use this project output to predict if a museum will be popular with market, and study what factors make a museum popular.

Museum staff can get intuitve feedback on how visitors view their museum.


## Other Scenarios
1. If the data was increased by 100x.
2. If the pipelines would be run on a daily basis by 7 am every day.
3. If the database needed to be accessed by 100+ people.




 