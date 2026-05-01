use role ACCOUNTADMIN;


-- Q3: Create Database

create database if not exists flight_analysis
    DATA_RETENTION_TIME_IN_DAYS = 1;

use database flight_analysis;


-- Q4: Create Schema

create schema if not exists staging
    DATA_RETENTION_TIME_IN_DAYS = 1;

use schema staging;


-- Q5 + Q6: Stage with Parquet Format
-- Note: IAM Role Free Trial mai kaam nahi kiya
-- isliye AWS Access Keys use kiye

create or replace stage gold_stage_s3
    URL         = 's3://flight-analytics-mitansh/gold_layer/'
    CREDENTIALS = (AWS_KEY_ID     = 'Aws key'
                   AWS_SECRET_KEY = 'aws secret key')
    FILE_FORMAT = (TYPE = 'PARQUET');


-- Q7: List Stage -- Verify Files

list @gold_stage_s3;


-- Q8: monthly_airline_kpi
-- 4 group by columns + saare KPI columns

create or replace table monthly_airline_kpi (
    -- 4 Group By Columns jo silver table mai use kiye the
    flight_year                 INTEGER,
    flight_month                INTEGER,
    airline_code                VARCHAR,
    reporting_airline           VARCHAR,
    -- Basic Flight Metrics
    total_flights               BIGINT,
    delayed_flights             BIGINT,
    total_flights_cancelled     BIGINT,
    total_delay_minutes         DOUBLE,
    total_diverted              BIGINT,
    -- Delay Metrics
    avg_arr_delay_minutes       DOUBLE,
    median_arrival_delay        DOUBLE,
    -- On Time Metrics
    on_time_flights             BIGINT,
    on_time_flight_percentage   DOUBLE,
    cancelled_flight_percentage DOUBLE,
    -- Delay Type Metrics
    avg_carrier_delay           DOUBLE,
    avg_weather_delay           DOUBLE,
    avg_security_delay          DOUBLE,
    avg_late_aircraft_delay     DOUBLE,
    -- Distance Metrics
    avg_distance_travelled      DOUBLE,
    total_distance_travelled    DOUBLE,
    -- Rank
    monthly_rank                INTEGER
)
DATA_RETENTION_TIME_IN_DAYS = 1;


-- Q9: Retention Check
-- Free Trial mai max 1 day hota hai
-- 10 days Enterprise Edition mai hota hai

select TABLE_NAME, RETENTION_TIME
from   information_schema.tables
where  TABLE_SCHEMA = 'STAGING'
  and  TABLE_NAME   = 'MONTHLY_AIRLINE_KPI';


-- Q10: annual_route_performance
-- Route based data -- same columns as Databricks

create or replace table annual_route_performance (
    -- Group By Columns
    flight_year                           INTEGER,
    route                                 VARCHAR,    -- e.g. JFK-LAX
    origin_code                           VARCHAR,
    destination_code                      VARCHAR,
    -- KPI Metrics
    number_of_flights                     BIGINT,
    avg_arrival_delay                     DOUBLE,
    avg_distance_travelled                DOUBLE,
    total_delayed_flights                 BIGINT,
    number_of_airlines_on_route           BIGINT,
    on_time_percentage_airline_percentage DOUBLE
)
DATA_RETENTION_TIME_IN_DAYS = 1;

-- Retention Verify
select TABLE_NAME, RETENTION_TIME
from   information_schema.tables
where  TABLE_SCHEMA = 'STAGING'
  and  TABLE_NAME   = 'ANNUAL_ROUTE_PERFORMANCE';


-- Q11: 4 More Tables
-- Same as Gold Layer in Databricks


-- Table 1: airport_departure_kpi
create or replace table airport_departure_kpi (
    -- Group By Columns
    flight_year                  INTEGER,
    flight_month                 INTEGER,
    origin_code                  VARCHAR,
    -- Airport Info
    name                         VARCHAR,
    city                         VARCHAR,
    state                        VARCHAR,
    lon                          DOUBLE,
    lat                          DOUBLE,
    -- Departure Metrics
    total_departure              BIGINT,
    total_cancelled_departure    BIGINT,
    avg_delayed_departure        DOUBLE,
    avg_route_distance           DOUBLE,
    number_of_flights_operating  BIGINT,
    avg_airtime                  DOUBLE,
    departure_on_time_percentage DOUBLE,
    -- Year Month
    year_month                   VARCHAR     
)
DATA_RETENTION_TIME_IN_DAYS = 1;

-- Table 2: delay_cause_table
create or replace table delay_cause_table (
    -- Group By Columns
    flight_year                         INTEGER,
    flight_month                        INTEGER,
    airline_code                        VARCHAR,
    -- Total Delay Minutes
    total_minutes_delayed               DOUBLE,
    -- Delay Type Minutes
    total_weather_delayed_minutes       DOUBLE,
    total_carrier_delayed_minutes       DOUBLE,
    total_security_delayed_minutes      DOUBLE,
    total_late_aircraft_delayed_minutes DOUBLE,
    -- Delay Type Percentages
    weather_delay_percentage            DOUBLE,
    carrier_delay_percentage            DOUBLE,
    security_delay_percentage           DOUBLE,
    late_aircraft_delay_percentage      DOUBLE
)
DATA_RETENTION_TIME_IN_DAYS = 1;

-- Table 3: airline_flight_metrics
create or replace table airline_flight_metrics (
    -- Group By Columns
    flight_year             INTEGER,
    flight_month            INTEGER,
    airline_code            VARCHAR,
    reporting_airline       VARCHAR,
    -- Flight Metrics
    total_number_of_flights BIGINT,
    total_delayed_flights   BIGINT,
    total_flights_cancelled BIGINT,
    total_flights_diverted  BIGINT
)
DATA_RETENTION_TIME_IN_DAYS = 1;

-- Table 4: airline_delay_distance_metrics
create or replace table airline_delay_distance_metrics (
    -- Group By Columns
    flight_year              INTEGER,
    flight_month             INTEGER,
    airline_code             VARCHAR,
    reporting_airline        VARCHAR,
    -- Avg Delay Metrics
    avg_carrier_delay        DOUBLE,
    avg_weather_delay        DOUBLE,
    avg_security_delay       DOUBLE,
    avg_late_aircraft_delay  DOUBLE,
    -- Distance Metrics
    avg_distance_travelled   DOUBLE,
    total_distance_travelled DOUBLE
)
DATA_RETENTION_TIME_IN_DAYS = 1;


-- COPY INTO -- Data Load karo S3 se


-- Load monthly_airline_kpi
copy into monthly_airline_kpi
from @gold_stage_s3/airline_api_1g/
FILE_FORMAT         = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN             = '.*\.parquet';

-- Load annual_route_performance
copy into annual_route_performance
from @gold_stage_s3/gold_route_KPI_2g/
FILE_FORMAT         = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN             = '.*\.parquet';

-- Load airport_departure_kpi
copy into airport_departure_kpi
from @gold_stage_s3/gold_airport_departure_kpi_3g/
FILE_FORMAT         = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN             = '.*\.parquet';

-- Load delay_cause_table
copy into delay_cause_table
from @gold_stage_s3/delay_cause_table_4g/
FILE_FORMAT         = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN             = '.*\.parquet';


-- Final Verify -- Saari tables ek saath dekho

select TABLE_NAME, RETENTION_TIME
from   information_schema.tables
where  TABLE_SCHEMA = 'STAGING'
order by TABLE_NAME;

select count(*) AS monthly_airline_kpi_rows      FROM monthly_airline_kpi;
SELECT COUNT(*) AS annual_route_performance_rows  FROM annual_route_performance;
SELECT COUNT(*) AS airport_departure_kpi_rows     FROM airport_departure_kpi;
SELECT COUNT(*) AS delay_cause_table_rows         FROM delay_cause_table;
SELECT COUNT(*) AS airline_flight_metrics_rows    FROM airline_flight_metrics;
SELECT COUNT(*) AS airline_delay_distance_rows    FROM airline_delay_distance_metrics;


select * from monthly_airline_kpi LIMIT 5;
select * from annual_route_performance limit 5;
select * from airport_departure_kpi limit 5;
select * from delay_cause_table limit 5;
