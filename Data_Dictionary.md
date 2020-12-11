# Data Dictionary


## Source data
Datasets are collected from Kaggle and Tripadvisor and are uploaded to AWS S3 `s3://udacity-dend-shell845/museum-data/`

1. **museum** overall summary of museums extracted from Tripadvisor. In csv format.

	```
mid,Address,Description,FeatureCount,Fee,Langtitude,Latitude,LengthOfVisit,MuseumName,PhoneNum,Rank,Rating,ReviewCount,TotalThingsToDo
0,"555 Pennsylvania Ave NW, Washington DC, DC 20001-2114","Find out for yourself why everyone is calling the Newseum the best experience Washington, D.C. has to offer. Each of the seven levels in this magnificent building is packed with interactive exhibits that explore free expression and the five freedoms of the First Amendment: religion, speech, press, assembly and petition. Whether you have just a few hours or want to spend all day, you'll find something for everyone in the family in the Newseum's 15 theaters and 15 galleries.",3,Yes ,-77.0192351,38.8931385,2-3 hours ,Newseum,+1 888-639-7386,8,4.5,"6,309",398
	```

    
2. **category** categories of museums, e.g. art museum, history museum, science museum etc. In json format.

    ```
	{'museum': ['museum type 1','museum type 2', …]}
	{"Gettysburg Heritage Center": ["History Museums", "Museums"]}
	```


3. **rating** how many ratings did the museums receive and what are the ratings. In json format.

    ```
	{'museum': ['Excellent','Very good','Average','Poor','Terrible']}
	{"Gettysburg Heritage Center": ["164", "63", "10", "5", "4"]}
	```
	
    
4. **traveler** how the travers travel. In json format.
   
   ```
	{'museum': ['Families','Couples','Solo','Business','Friends']}
	{"Gettysburg Heritage Center": ["88", "86", "17", "2", "33"]}
	```
    
    
5. **weather** average temperature of the cities where the museums are located. In csv format.

   ```
	dt,AverageTemperature,AverageTemperatureUncertainty,City,Country,Latitude,Longitude
	2008-01-01,10.915999999999999,0.165,Pasadena,United States,29.74N,96.00W
	```


## Staging, fact and dimension tables

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

Please refer to below metadata of all tables as data dictionary:

### Staging table
`staging_category` load data from **category**

| Field Name | Data Type | Description                          | Example                      |Primary Key|Data source|
|------------|-----------|--------------------------------------|------------------------------|-|-|
| museum     | text      | museum name                          | National Museum of Arts      | N|category data file|
| category   | text      | museum category                      | Arts Museum                  | N|category data file|

`staging_traveler` load data from **traveler**

| Field Name | Data Type | Description                          | Example                      |Primary Key|Data source|
|------------|-----------|--------------------------------------|------------------------------|-|-|
| museum     | text      | museum name                          | National Museum of Arts      |N|traveler data file|
| type       | text      | traveler type                        | Business                     |N|traveler data file|
| number     | integer   | number of travellers                  | 283                          |N|traveler data file|

`staging_weather` load data from **weather**

| Field Name | Data Type | Description                          | Example                      |Primary Key|Data source|
|------------|-----------|--------------------------------------|------------------------------|-|-|
| dt         | date      | weather date                         | 2013-10-01                   |N|weather data file|
| averageTemperature | FLOAT | average temperature of this month| 18.39                        |N|weather data file|
| city       | text      | city                                 | Toronto                      |N|weather data file|
| country    | text      | country                              | Canada                       |N|weather data file|

`staging_museum` load data from **museum**

| Field Name | Data Type | Description                          | Example                      |Primary Key|Data source|
|------------|-----------|--------------------------------------|------------------------------|-|-|
| museumname | date      | museum name                          | National Museum of Arts      |N|museum data file|
| rating     | FLOAT     | overall rating of the museum         | 4.6                          |N|museum data file|
| city       | text      | city                                 | Toronto                      |N|museum data file|
| address    | text      | full address of the museum           | Canada                       |N|museum data file|


### Fact table
#### `museum_fact`

| Field Name | Data Type | Description                          | Example                      |Primary Key|Data source|
|------------|-----------|--------------------------------------|------------------------------|-|-|
| fact_id    | bigserial | primary key, id of the museum fact   | 2904                         |Y|self increment|
| museum_id  | integer   | museum id                            | 38                           |N|museum dimension table, foreign key of museum.museum_id|
| city_id    | integer   | city id of museum location           | 72                           |N|city dimension table, foreign key of city.city_id|
| category_id| integer   | id of museum category                | 2                            |N|city dimension table, foreign key of city.city_id|
| rating     | numeric   | average rating given by visitors     | 4.7                          |N|museum dimension table|
| traveler_type_id|integer| id of visitor type                  | 3                            |N|traveler dimension table, foreign key of traveler.traveler_type_id|
| weather    | numeric   | average temperature of the city      | 17.4                         |N|weather dimension table|
| dt         | date      | weather date                         | 2013-10-01                   |N|user input date|


### Dimension table
#### `city`
| Field Name | Data Type | Description                 | Example      |Primary Key|Data source|
|------------|-----------|-----------------------------|--------------|-|-|
| city_id    | bigserial | primary key, unique city id | 28           |Y|self increment|
| city_name  | text      | city name                   | New York     |N|staging_museum table|
| country    | text      | city country                | United State |N|user input|

#### `category`
| Field Name  | Data Type | Description                            | Example     |Primary Key|Data source|
|-------------|-----------|----------------------------------------|-------------|-|-|
| category_id | serial    | primary key, unique id of the category | 29          |Y|self increment|
| category    | text      | museum category                        | Arts museum |N|staging_category table|

#### `traveler`
| Field Name | Data Type | Description   | Example  |Primary Key|Data source|
|------------|-----------|---------------|----------|-|-|
| type_id    | serial    | primary key   | 1        |Y|self increment|
| type       | text      | traveler type | Families |N|staging_traveler table|

#### `weather`
| Field Name | Data Type | Description                    | Example    |Primary Key|Data source|
|------------|-----------|--------------------------------|------------|-|-|
| city_id    | integer   | refers to city_id of city      | 1          |N|city table|
| weather_date| date     | date of weather                | 2019-06-01 |N|user input|
| weather    | numeric   | average temperature in celsius | 17.8       |N|staging_weather table|

#### `museum`
| Field Name   | Data Type | Description                   | Example                        |Primary Key|Data source|
|--------------|-----------|-------------------------------|--------------------------------|-|-|
| museum_id    | bigserial | primary key, unique museum id | 281                            |Y|self increment|
| museum_name  | text      | museum name                   | The Metropolitan Museum of Art |N|museum table|
| category_id  | integer   | what type of museum           | 2                              |N|category table, foreign key of category.category_id|
| full_address | text      | full address of the museum    | 1000 Fifth Avenue              |N|staging_museum table|
| city_id      | integer   | city id                       | 827                            |N|city table, foreign key of city.city_id|
| rating       | numeric   | average rating given by visitors   | 4.7                       |N|staging_museum table|
| traveler     | text      | the museum is most popular among which traveler group | 4.7    |N|traveler table and staging_museum table|


## DB diagram

![DB Diagram](DB-diagram.png)