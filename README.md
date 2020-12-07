## Project summary
The purpose of this project is to collect and present information about museums (US only at current stage, may extend to global in next pharse), including museum categories, locations, ratings and more. 

## Source data
Datasets are collected from Kaggle and Tripadvisor and are uploaded to AWS S3 `s3://udacity-dend-shell845/museum-data/`

1. **museum** overall summary of museums extracted from Tripadvisor. In csv format.

	`Address, Description, FeatureCount, Fee, Langtitude, Latitude, LengthOfVisit, MuseumName, PhoneNum, Rank, Rating, ReviewCount,TotalThingsToDo`
	
2. **category** categories of museums, e.g. art museum, history museum, science museum etc. In json format.
	
	`{'museum': ['museum type 1','museum type 2', …]}`
	
3. **rating** how many ratings did the museums receive and what are the ratings. In json format.

	`{'museum': ['Excellent','Very good','Average','Poor','Terrible']}`


4. **traveler** how the travers travel. In json format.

	`{'museum': ['Families','Couples','Solo','Business','Friends']}`

5. **weather** average tempature of the cities where the museums are located. In csv format.

	`dt,AverageTemperature,AverageTemperatureUncertainty,City,Country,Latitude,Longitude`


## Data model
### Staging table
1. `staging_category` load data from **category**
2. `staging_rating` load data from **rating**
3. `staging_traveler` load data from **traveler**
4. `staging_weather` load data from **weather**
3. `staging_museum` load data from **museum**

### Fact table
#### museum_fact
| Field Name | Data Type | Description                          | Example                      |
|------------|-----------|--------------------------------------|------------------------------|
| fact_id    | bigserial | primary key, unique id of the museum fact | 2904                    |
| museum_id  | integer   | museum id                            | 38                           |
| city_id    | text      | museum location (city)               | Montreal                     |
| category_id| text      | what kind of museum it is            | Arts                         |
| rating     | numeric   | average rating given by visitors     | 4.7                          |
| traveler_type_id| text | what visitor visit the museum        | Couples                      |
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


### Relationship diagram
![DB-diagram](DB-diagram.png)

## ETL pipeline

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




 