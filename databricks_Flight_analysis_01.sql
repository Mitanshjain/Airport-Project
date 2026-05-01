-- DATABASE + SCHEMA

use database flight_analysis;

-- create a new schema with dimesions name 
create schema if not exists dimensions;

use schema dimensions;


use warehouse COMPUTE_WH;

-- AIRLINE DIMENSION -- SCD Type 2
-- Real data from monthly_airline_kpi
create or replace table airline_dimension (
    airline_sk           INTEGER AUTOINCREMENT,
    airline_code         VARCHAR,
    airline_name         VARCHAR,
    carrier_plane        VARCHAR,
    airport_hub          VARCHAR,
    effective_start_date DATE,
    effective_end_date   DATE,
    is_current           BOOLEAN
);

insert into airline_dimension (
    airline_code,
    airline_name,
    carrier_plane,
    airport_hub,
    effective_start_date,
    effective_end_date,
    is_current
)
select distinct  airline_code,CASE airline_code
        when 'F9' then 'Frontier Airlines'
        when 'YV' then 'Mesa Airlines'
        when 'AA' then 'American Airlines'
        when 'NK' then 'Spirit Airlines'
        when 'OH' then 'PSA Airlines'
        when 'YX' then 'Republic Airways'
        when 'AS' then 'Alaska Airlines'
        when 'MQ' then 'Envoy Air'
        when 'DL' then 'Delta Air Lines'
        when 'UA' then 'United Airlines'
        when '9E' then 'Endeavor Air'
        when 'HA' then 'Hawaiian Airlines'
        when 'QX' then 'Horizon Air'
        when 'OO' then 'SkyWest Airlines'
        when 'WN' then 'Southwest Airlines'
        when 'B6' then 'JetBlue Airways'
        when 'G4' then 'Allegiant Air'
        else airline_code
    end                as airline_name,
    NULL               as carrier_plane,
    NULL               as airport_hub,
    CURRENT_DATE()     as effective_start_date,
    NULL               as effective_end_date,
    TRUE               as is_current
from flight_analysis.staging.monthly_airline_kpi;


-- Verify
select * from airline_dimension;

-- NUMBER SERIES -- 1 to 100
create or replace table number_series as
select row_number() over (order by SEQ4()) as num
from table(GENERATOR(ROWCOUNT => 100));

-- Verify
select * from number_series;

-- DATE DIMENSION
-- 1 Jan 2021 to 31 Dec 2022
-- 720-730 days (leap year consider kiya)

create or replace table date_dimension (
    full_date          DATE,
    year_number        INTEGER,
    month_number       INTEGER,
    month_name         VARCHAR,
    day_of_week_number INTEGER,
    day_of_week_name   VARCHAR,
    quarter_number     INTEGER
);

-- insert base date
insert into date_dimension
SELECT
    generated_date                AS full_date,
    YEAR(generated_date)          AS year_number,
    MONTH(generated_date)         AS month_number,
    MONTHNAME(generated_date)     AS month_name,
    DAYOFWEEKISO(generated_date)  AS day_of_week_number,
    DAYNAME(generated_date)       AS day_of_week_name,
    QUARTER(generated_date)       AS quarter_number
from (
    select
        DATEADD(DAY, SEQ4(), '2021-01-01') AS generated_date
    from table(GENERATOR(ROWCOUNT => 750))
)
where generated_date < '2023-01-01';


-- Verify base data
select * from date_dimension limit 20;

-- in this date_dimension table add is_weekend column 
-- Monday=1, Tuesday=2 ... Saturday=6, Sunday=7
-- Weekend = Saturday(6) or Sunday(7) = TRUE
alter table date_dimension
add column is_weekend boolean;


update date_dimension
set is_weekend =
    case
        when day_of_week_number in (6, 7) then TRUE
        else FALSE
    end;


-- Verify
select
    full_date,
    day_of_week_number,
    day_of_week_name,
    is_weekend
from date_dimension
limit 20;

-- Add new column in date_dimension table with the name of season
alter table date_dimension
add column season varchar;


update date_dimension
set season =
    case
        when month_number IN (12, 1, 2) then 'Winter'
        when month_number IN (3, 4)     then 'Spring'
        when month_number IN (5, 6, 7)  then 'Summer'
        when month_number IN (9, 10)    then 'Monsoon'
        else                                 'Autumn'
    end;



-- Verify
select distinct
    month_number,
    month_name,
    season
from date_dimension
order by month_number;

-- Add new column with the name of year_month
-- Format: 2021-01, 2021-02 etc
-- by using Lpad get 2 digit month


alter table date_dimension
add column year_month varchar;

update date_dimension
set year_month =
    CONCAT(
        year_number,
        '-',
        lpad(month_number, 2, '0')
    );

-- Verify
select
    full_date,
    year_number,
    month_number,
    year_month
from date_dimension
limit 20;