# ✈️ US Flight Analytics — Data Engineering Project

A complete end-to-end Data Engineering project that ingests, transforms, and analyzes US domestic flight data (2021–2022) using **Apache Spark (Databricks)**, **AWS S3**, **Delta Lake**, and **Snowflake**.

---

## 📁 Project Structure

```
US-Flight-Analytics/
│
├── Airport_Data.py                   # Phase 1 — S3 bucket setup + data upload using boto3
├── Final-Airport.py                  # Phase 2 — Bronze layer ingestion + schema + data quality
├── Final-Airport-09-04-26.py         # Phase 2 (Final) — Bronze layer with corrupt record handling
├── Phase_3_Flight.py                 # Phase 3 — Silver & Gold layer transformations
│
├── databricks_Flight_analysis.sql    # Snowflake — Staging schema + tables + COPY INTO
├── databricks_Flight_analysis_01.sql # Snowflake — Dimensions schema + airline/date dimension
│
└── README.md                         # Project documentation (this file)
```

---

## 🏗️ Architecture Overview

```
Raw CSV Files (BTS Website)
        │
        ▼
  AWS S3 Bucket                          ← boto3 upload (Airport_Data.py)
  s3://flight-analytics-mitansh/
        │
        ▼
  ┌─────────────────────────────────┐
  │        DATABRICKS               │
  │                                 │
  │  Bronze Layer  (Raw + Metadata) │  ← Final-Airport.py
  │        │                        │
  │  Silver Layer  (Cleaned)        │  ← Phase_3_Flight.py
  │        │                        │
  │  Gold Layer    (Aggregated KPI) │  ← Phase_3_Flight.py
  └─────────────────────────────────┘
        │
        ▼
  AWS S3 Gold Layer (Parquet files)
  s3://flight-analytics-mitansh/gold_layer/
        │
        ▼
  ┌─────────────────────────────────┐
  │        SNOWFLAKE                │
  │                                 │
  │  Database: flight_analysis      │
  │  ├── Schema: staging            │  ← databricks_Flight_analysis.sql
  │  │   ├── monthly_airline_kpi    │
  │  │   ├── annual_route_perf...   │
  │  │   ├── airport_departure_kpi  │
  │  │   └── delay_cause_table      │
  │  │                              │
  │  └── Schema: dimensions         │  ← databricks_Flight_analysis_01.sql
  │      ├── airline_dimension      │
  │      ├── date_dimension         │
  │      └── airline_date_dim...    │
  └─────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| **Apache Spark (PySpark)** | Data processing and transformations |
| **Databricks Free Edition** | Spark execution environment |
| **AWS S3** | Data lake storage (Bronze/Silver/Gold) |
| **Delta Lake** | ACID transactions on S3 |
| **Snowflake** | Data warehouse for analytics |
| **boto3** | Python SDK for AWS S3 uploads |
| **Python** | Scripting and automation |

---

## 📂 Data Source

- **Source**: US Bureau of Transportation Statistics (BTS)
- **Dataset**: On-Time Reporting Carrier On-Time Performance
- **Years**: 2021 and 2022
- **Format**: CSV files (partitioned by Year/Month)
- **Total Columns**: 110 columns per record
- **S3 Path**: `s3://flight-analytics-mitansh/flight-data/YYYY/MM/`

---

## 🔄 Phase-by-Phase Explanation

### Phase 1 — Data Upload to S3 (`Airport_Data.py`)

**What we did:**
- Created AWS S3 bucket: `flight-analytics-mitansh`
- Used `boto3` (Python AWS SDK) to upload raw CSV flight data files
- Uploaded airport metadata JSON file (`airports_multiline.json`)
- Data organized in partitioned folder structure: `flight-data/YYYY/MM/`

**Key Concepts:**
- `boto3.client('s3')` — connects Python to AWS S3
- Partition structure helps Spark read only required year/month (faster!)

---

### Phase 2 — Bronze Layer (`Final-Airport.py` / `Final-Airport-09-04-26.py`)

**What we did:**

#### Step 1 — Read Raw CSV with InferSchema
```python
df = spark.read.format("csv") \
    .option("header", "true") \
    .load("s3://flight-analytics-mitansh/flight-data/*/*/*")
```
> Used `inferSchema` first just to check column data types before defining manual schema.

#### Step 2 — Define Manual Schema (110 columns)
- Created `StructType` with `StructField` for all 110 columns
- Intentionally set `Flight_Number_Reporting_Airline` as `IntegerType` to demonstrate schema mismatch
- Used `StringType`, `IntegerType`, `DoubleType`, `DateType`

#### Step 3 — Select 36 Important Columns
```python
cols_36 = ["FlightDate", "Reporting_Airline", "Origin", "Dest",
           "DepDelay", "ArrDelay", "Cancelled", "Distance", ...]
df = df_full.select(cols_36)
```

#### Step 4 — Read Modes
| Mode | Behavior |
|------|---------|
| `PERMISSIVE` | Reads all rows, puts bad rows in `_corrupt_record` column |
| `DROPMALFORMED` | Drops rows that cannot be parsed |
| `FAILFAST` | Fails immediately on first bad row |

#### Step 5 — Null Value Analysis
```python
# Check null % for important columns
cols_check = ["Distance", "DepDelay", "Reporting_Airline", "Origin", "Cancelled", "Dest"]
for c in cols_check:
    null_count = df.filter(col(c).isNull()).count()
    null_percent = (null_count / total_count) * 100
```

#### Step 6 — Data Quality Flags
```python
# Flag 1: Departure delay validity
df = df.withColumn("dep_delay_flag",
    when((col("DepDelay") >= -120) & (col("DepDelay") <= 1440), 0).otherwise(1))

# Flag 2: Same origin and destination
df = df.withColumn("same_origin_destination",
    when(col("Origin") == col("Dest"), 1).otherwise(0))

# Flag 3: Domestic flight distance check
df = df.withColumn("is_domestic_flag",
    when((col("Distance") >= 1) & (col("Distance") <= 10000), 0).otherwise(1))
```

#### Step 7 — Add Metadata + Save as Bronze Delta
```python
bronze_df = df_final \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("source_file", col("_metadata.file_path"))

bronze_df.write.format("delta").mode("overwrite") \
    .partitionBy("year", "month") \
    .save("s3a://flight-analytics-mitansh/bronze/")
```

#### Step 8 — Airport JSON Data
```python
# Read multiline JSON for airport metadata
df_airport = spark.read.option("multiline", "true").schema(schema) \
    .json("s3://flight-analytics-mitansh/flight-data/airports_multiline.json")

# Filter only US airports
df_us = df_airport.filter(col("country") == "US")

# Save as Delta
df_us.write.format("delta").mode("overwrite") \
    .save("s3://flight-analytics-mitansh/delta/Airport")
```

---

### Phase 3 — Silver & Gold Layer (`Phase_3_Flight.py`)

**What we did:**

#### Silver Layer — Data Cleaning
- Loaded Bronze Delta table from S3
- Applied transformations:
  - Renamed columns for consistency
  - Created boolean flags: `is_cancelled`, `is_diverted`, `is_arrival_delayed`, `is_departure_delayed`
  - Added `flight_year`, `flight_month`, `airline_code`, `origin_code`, `destination_code`
  - Joined with Airport data for lat/lon/city/state info
- Saved as Silver Delta table: `s3://flight-analytics-mitansh/mitansh-flight/flight/df_silver/`

#### Gold Layer — KPI Aggregations

**4 Gold DataFrames created:**

| DataFrame | Group By | Key Metrics |
|-----------|---------|-------------|
| `df_gold` | year, month, airline_code, reporting_airline | total_flights, delayed_flights, cancelled %, on_time %, avg_delay, monthly_rank |
| `df_gold_route_kpi` | year, route (JFK-LAX), origin, destination | number_of_flights, avg_arrival_delay, on_time_percentage |
| `df_gold_airport_departure_kpi` | year, month, origin_code, airport_name | total_departures, cancelled_departures, avg_departure_delay |
| `df_delay_cause_table` | year, month, airline_code | weather_delay %, carrier_delay %, security_delay % |

**Saved to S3:**
```
s3://flight-analytics-mitansh/gold_layer/airline_api_1g/
s3://flight-analytics-mitansh/gold_layer/gold_route_KPI_2g/
s3://flight-analytics-mitansh/gold_layer/gold_airport_departure_kpi_3g/
s3://flight-analytics-mitansh/gold_layer/delay_cause_table_4g/
```

**Performance Comparison (Caching):**
```python
# Without cache
start_time = time.time()
df_silver.count()  # Slower — reads from S3 every time
time_taken = time.time() - start_time

# With Temp View (Free Edition alternative to cache)
df_silver.createOrReplaceTempView("df_silver_cached")
df_silver.count()  # Faster — Photon engine optimization
```

---

## ❄️ Snowflake Setup

### Database & Schema
```sql
CREATE DATABASE IF NOT EXISTS flight_analysis
    DATA_RETENTION_TIME_IN_DAYS = 1;  -- Free Trial max = 1 day (Enterprise = 10 days)

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dimensions;
```

### Stage Setup (AWS Access Keys method)
> **Note:** IAM Role method failed in Snowflake Free Trial due to cross-account `sts:AssumeRole` restrictions.  
> Used AWS Access Keys as an alternative — simpler and works in Free Trial.

```sql
CREATE OR REPLACE STAGE gold_stage_s3
    URL         = 's3://flight-analytics-mitansh/gold_layer/'
    CREDENTIALS = (AWS_KEY_ID = '...' AWS_SECRET_KEY = '...')
    FILE_FORMAT = (TYPE = 'PARQUET');

LIST @gold_stage_s3;  -- Verify files are visible
```

### Tables in Staging Schema

| Table | Description |
|-------|-------------|
| `monthly_airline_kpi` | Monthly KPI per airline (21 columns) |
| `annual_route_performance` | Route-level performance (10 columns) |
| `airport_departure_kpi` | Airport departure metrics (16 columns) |
| `delay_cause_table` | Delay cause breakdown (12 columns) |
| `airline_flight_metrics` | Basic flight counts per airline |
| `airline_delay_distance_metrics` | Avg delay types + distance |

### Load Data using COPY INTO
```sql
COPY INTO monthly_airline_kpi
FROM @gold_stage_s3/airline_api_1g/
FILE_FORMAT          = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN              = '.*\.parquet';
```

### Dimensions Schema

#### Airline Dimension (SCD Type 2)
```sql
CREATE OR REPLACE TABLE airline_dimension (
    airline_sk           INTEGER AUTOINCREMENT,  -- Surrogate Key
    airline_code         VARCHAR,                -- Natural Key (AA, DL, UA...)
    airline_name         VARCHAR,                -- American Airlines, Delta...
    carrier_plane        VARCHAR,
    airport_hub          VARCHAR,
    effective_start_date DATE,                   -- SCD Type 2: record valid from
    effective_end_date   DATE,                   -- SCD Type 2: record valid to
    is_current           BOOLEAN                 -- TRUE = current record
);
```
> Real airline data pulled from `staging.monthly_airline_kpi` table.

#### Date Dimension
```sql
-- 730 rows: 1 Jan 2021 to 31 Dec 2022
CREATE OR REPLACE TABLE date_dimension (
    full_date          DATE,
    year_number        INTEGER,
    month_number       INTEGER,
    month_name         VARCHAR,      -- January, February...
    day_of_week_number INTEGER,      -- ISO: Mon=1, Sun=7
    day_of_week_name   VARCHAR,      -- Monday, Tuesday...
    quarter_number     INTEGER,      -- 1, 2, 3, 4
    is_weekend         BOOLEAN,      -- Sat(6) or Sun(7) = TRUE
    season             VARCHAR,      -- Winter/Spring/Summer/Monsoon/Autumn
    year_month         VARCHAR       -- 2021-01, 2021-02...
);
```

**Season mapping:**
| Months | Season |
|--------|--------|
| 12, 1, 2 | ❄️ Winter |
| 3, 4 | 🌸 Spring |
| 5, 6, 7 | ☀️ Summer |
| 9, 10 | 🌧️ Monsoon |
| 8, 11 | 🍂 Autumn |

**Weekend logic:**
- ISO week starts Monday = 1
- Saturday = 6, Sunday = 7 → `is_weekend = TRUE`

---

## 🔑 Key Concepts Learned

| Concept | Where Used |
|---------|-----------|
| `StructType` / `StructField` | Manual schema definition for 110 columns |
| `PERMISSIVE` / `DROPMALFORMED` / `FAILFAST` | CSV read modes |
| `_corrupt_record` | Capturing bad rows |
| Delta Lake | ACID transactions on S3 |
| Medallion Architecture | Bronze → Silver → Gold |
| `partitionBy("year", "month")` | Faster partition reads |
| `createOrReplaceTempView` | Caching alternative in Free Edition |
| `percentile_approx(col, 0.5)` | Median calculation in Spark |
| `Window + dense_rank()` | Monthly airline ranking |
| SCD Type 2 | Historical tracking with effective dates |
| `COPY INTO` | Bulk load from S3 stage to Snowflake |
| `LPAD(month, 2, '0')` | Format month as 2 digits (01, 02...) |
| `DAYOFWEEKISO` | ISO week numbering (Mon=1) |
| `GENERATOR(ROWCOUNT)` | Generate rows in Snowflake |

---

## ⚠️ Issues Faced & Solutions

### 1. IAM Role AssumeRole Error in Snowflake
**Error:** `User: arn:aws:iam::360999005803:user/l82n1000-s is not authorized to perform: sts:AssumeRole`

**Root Cause:** Snowflake Free Trial uses cross-account role assumption which requires specific trust policy configuration.

**Solution:** Used **AWS Access Keys** directly in stage credentials instead of Storage Integration.

---

### 2. Data Retention Time Error
**Error:** `Exceeds maximum allowable retention time (1 day(s))`

**Root Cause:** Snowflake Free Trial limits retention to 1 day. 10 days requires Enterprise Edition.

**Solution:** Set `DATA_RETENTION_TIME_IN_DAYS = 1` — concept same, only edition limitation.

---

### 3. `%pip install` Core Package Warning
**Warning:** `requests: 2.32.3 -> 2.33.1` version changed

**Solution:** Run `dbutils.library.restartPython()` after pip install — no data loss.

---

### 4. Cache Not Working in Serverless Free Edition
**Issue:** `df.cache()` and `df.persist()` not supported in Databricks Serverless Free Edition.

**Solution:** Used `createOrReplaceTempView()` — Photon engine internally optimizes repeated reads.

---

## 📊 Gold Layer KPI Summary

### monthly_airline_kpi
- Grouped by: `flight_year`, `flight_month`, `airline_code`, `reporting_airline`
- Metrics: total_flights, delayed_flights, cancelled %, on_time %, avg delays, distance, `monthly_rank`

### annual_route_performance
- Grouped by: `flight_year`, `route` (e.g. JFK-LAX), `origin_code`, `destination_code`
- Metrics: number_of_flights, avg_arrival_delay, on_time_percentage, number_of_airlines_on_route

### airport_departure_kpi
- Grouped by: `flight_year`, `flight_month`, `origin_code`, airport info (name, city, state, lat, lon)
- Metrics: total_departures, cancelled_departures, avg_departure_delay, avg_airtime, `year_month`

### delay_cause_table
- Grouped by: `flight_year`, `flight_month`, `airline_code`
- Metrics: total_minutes_delayed, weather_delay %, carrier_delay %, security_delay %, late_aircraft_delay %

---

## 🗂️ File Reference

| File | Phase | Technology |
|------|-------|-----------|
| `Airport_Data.py` | Phase 1 | Python, boto3, AWS S3 |
| `Final-Airport.py` | Phase 2 | PySpark, Databricks, Delta Lake |
| `Final-Airport-09-04-26.py` | Phase 2 (Final) | PySpark, Databricks, Delta Lake |
| `Phase_3_Flight.py` | Phase 3 | PySpark, Delta Lake, Gold Layer |
| `databricks_Flight_analysis.sql` | Snowflake | Snowflake SQL, staging schema |
| `databricks_Flight_analysis_01.sql` | Snowflake | Snowflake SQL, dimensions schema |

---

## 👨‍💻 Author

**Mitansh Jain**  
Data Engineering Project — US Flight Analytics  
Technologies: PySpark · Databricks · AWS S3 · Delta Lake · Snowflake
