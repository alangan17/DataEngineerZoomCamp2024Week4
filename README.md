# DataEngineerZoomCamp2024Week4
Analytics Engineering using dbt &amp; BigQuery

[Slides](https://docs.google.com/presentation/d/1xSll_jv0T8JF4rYZvLHfkJXYqUjPtThA/edit#slide=id.p1)

## Lesson Learned

```mermaid
mindmap
    id1)Week 4: Analytics Engineering(
        
```

### 1. What is Analytics Engineering
- Data Engineer
  - Prepares and maintain the infrastructure the data team needs

- Data Analyst
  - Uses data to answer questions and solve business problems

- Analytics Engineer
  - Bridge the gap between Data Engineer and Data Analyst
  - Bring in good software engineering practices to the efforts of data analysts and data scientists
 
- Tools
  - Data Ingestion
    - Fivetran
    - Mage
  - Data Storage
    - Snowflake
    - Bigquery
    - Redshift
  - Data Modelling
    - dbt
    - Dataform
  - Data Visualization
    - Looker
    - Mode
    - Tableau

### 2. Data Modelling Concepts
- ETL vs ELT
  - ETL
    - Slightly more stable and compliant data analysis
    - Higher storage and compute costs
  - ELT
    - Faster and more flexible data analysis
    - Lower cost and lower maintenance
   
- Kimball's Dimensional Modelling
  - Deliver data understandable to business users
  - Deliver fast query performance
  - Fact tables (verbs)
  - Dimension tables (nouns)
  - Stages (think of a restaurant)
    - Stage Area/ Bronze (Raw data)
    - Processing Area/ Silver (raw data to data models)
    - Presentation Area/ Gold (Present data to users)

- Other modelling approaches
  - Inmon
  - Data Vault

### 3. What is dbt
- Data Transformation Tool
  - dbt Core
    - Open source and free to use
    - Build and run dbt projects (.sql and .yml files)
    - Compile models into sql (just select, no DDL/ DML) and execute in Database
    - CLI interface to run dbt commands locally
  - dbt Cloud
    - SaaS application to develop and manage dbt projects
    - Job orchestration
    - Logging and Alerting
    - Integrated documentation
    - Free for individuals

### 4. Starting a dbt project

### 5. Develop dbt models
- dbt models
  - jinja sql
  - compile to sql
  - materialization strategy
    - view
    - table
    - incremental (Update/ Insert table)
    - ephemeral
  - FROM
    - sources
      - physical tables/ views defined in yml files
    - seeds
      - static/ infrequently change CSV files
  - Macros
    - reusable SQL code
  - Variables
- packages.yml
  - like libraries of other programming languages
  - do not reinventing the wheel!
- project.yml

### 6. Testing and documenting dbt models

### 7. Deploy a dbt project

### 8. Visualization of the transformed data

## Module 4 Homework 

In this homework, we'll use the models developed during the week 4 videos and enhance the already presented dbt project using the already loaded Taxi data for fhv vehicles for year 2019 in our DWH.

This means that in this homework we use the following data:

CSV format: [Datasets list](https://github.com/DataTalksClub/nyc-tlc-data/)

Parquet format: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

* Yellow taxi data - Years 2019 and 2020
* 
* Green taxi data - Years 2019 and 2020

* fhv data - Year 2019.

### Homework Setup

#### Data Ingestion from data source to GCS
1. Create the buckets in GCS
2. Run the mage pipeline `api_to_gcs_bulk`

#### Build the source tables in BigQuery
```sql
CREATE OR REPLACE EXTERNAL TABLE `dez2024-413305.nytaxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dez2024-wk-nytaxi-mage-green/green_tripdata_*.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE dez2024-413305.nytaxi.green_tripdata AS
SELECT * FROM dez2024-413305.nytaxi.external_green_tripdata;
```

```sql
CREATE OR REPLACE EXTERNAL TABLE `dez2024-413305.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dez2024-wk-nytaxi-mage-yellow/yellow_tripdata_*.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE dez2024-413305.nytaxi.yellow_tripdata AS
SELECT * FROM dez2024-413305.nytaxi.external_yellow_tripdata;
```

```sql
CREATE OR REPLACE EXTERNAL TABLE `dez2024-413305.nytaxi.external_fhv_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dez2024-wk-nytaxi-mage-fhv/fhv_tripdata_*.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE dez2024-413305.nytaxi.fhv_tripdata AS
SELECT * FROM dez2024-413305.nytaxi.external_fhv_tripdata;
```


We will use the data loaded for:

* Building a source table: `stg_fhv_tripdata`
* Building a fact table: `fact_fhv_trips`
* Create a dashboard 

If you don't have access to GCP, you can do this locally using the ingested data from your Postgres database
instead. If you have access to GCP, you don't need to do it for local Postgres - only if you want to.

> **Note**: if your answer doesn't match exactly, select the closest option 

### Question 1: 

**What happens when we execute dbt build --vars '{'is_test_run':'true'}'**
You'll need to have completed the ["Build the first dbt models"](https://www.youtube.com/watch?v=UVI30Vxzd6c) video. 
- It's the same as running *dbt build*
- It applies a _limit 100_ to all of our models
- -> It applies a _limit 100_ only to our staging models
- Nothing

### Question 2: 

**What is the code that our CI job will run?**  

- The code that has been merged into the main branch
- The code that is behind the object on the dbt_cloud_pr_ schema
- The code from any development branch that has been opened based on main
- -> The code from a development branch requesting a merge to main


### Question 3 (2 points)

**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?**  
Create a staging model for the fhv data, similar to the ones made for yellow and green data. Add an additional filter for keeping only records with pickup time in year 2019.
Do not add a deduplication step. Run this models without limits (is_test_run: false).

Create a core model similar to fact trips, but selecting from stg_fhv_tripdata and joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run the dbt model without limits (is_test_run: false).

![alt text](</assets/question3.png>)

- 12998722
- -> 22998722
- 32998722
- 42998722

### Question 4 (2 points)

**What is the service that had the most rides during the month of July 2019 month with the biggest amount of rides after building a tile for the fact_fhv_trips table?**

Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, including the fact_fhv_trips data.

- FHV
- Green
- Yellow
- FHV and Green

### Problems encountered
#### data out of range

![Alt text](/assets/fhv_date_col_out_of_bound.png)


## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw4
