# Databricks notebook source
# Read data from s3 to databricks using catalog credentials
df = spark.read.format("csv") \
    .option("header", "true") \
    .load("s3://flight-analytics-mitansh/flight-data/*/*/*")

# COMMAND ----------

# just for knowing what types of columns we have for making user friendly schema.
df.columns

# COMMAND ----------

# We make a flight_schema in this we select all 110 columns 
from pyspark.sql.types import *

flight_schema = StructType([

    StructField("Year", IntegerType(), True),
    StructField("Quarter", IntegerType(), True),
    StructField("Month", IntegerType(), True),
    StructField("DayofMonth", IntegerType(), True),
    StructField("DayOfWeek", IntegerType(), True),

    StructField("FlightDate", StringType(), True),  # later cast to date

    StructField("Reporting_Airline", StringType(), True),
    StructField("DOT_ID_Reporting_Airline", IntegerType(), True),
    StructField("IATA_CODE_Reporting_Airline", StringType(), True),
    StructField("Tail_Number", StringType(), True),

    StructField("Flight_Number_Reporting_Airline", IntegerType(), True),  # ❗ intentional mistake (should be string)

    StructField("OriginAirportID", IntegerType(), True),
    StructField("OriginAirportSeqID", IntegerType(), True),
    StructField("OriginCityMarketID", IntegerType(), True),
    StructField("Origin", StringType(), True),
    StructField("OriginCityName", StringType(), True),
    StructField("OriginState", StringType(), True),
    StructField("OriginStateFips", StringType(), True),
    StructField("OriginStateName", StringType(), True),
    StructField("OriginWac", IntegerType(), True),

    StructField("DestAirportID", IntegerType(), True),
    StructField("DestAirportSeqID", IntegerType(), True),
    StructField("DestCityMarketID", IntegerType(), True),
    StructField("Dest", StringType(), True),
    StructField("DestCityName", StringType(), True),
    StructField("DestState", StringType(), True),
    StructField("DestStateFips", StringType(), True),
    StructField("DestStateName", StringType(), True),
    StructField("DestWac", IntegerType(), True),

    StructField("CRSDepTime", IntegerType(), True),
    StructField("DepTime", IntegerType(), True),
    StructField("DepDelay", DoubleType(), True),
    StructField("DepDelayMinutes", DoubleType(), True),
    StructField("DepDel15", DoubleType(), True),
    StructField("DepartureDelayGroups", IntegerType(), True),
    StructField("DepTimeBlk", StringType(), True),

    StructField("TaxiOut", DoubleType(), True),
    StructField("WheelsOff", IntegerType(), True),
    StructField("WheelsOn", IntegerType(), True),
    StructField("TaxiIn", DoubleType(), True),

    StructField("CRSArrTime", IntegerType(), True),
    StructField("ArrTime", IntegerType(), True),
    StructField("ArrDelay", DoubleType(), True),
    StructField("ArrDelayMinutes", DoubleType(), True),
    StructField("ArrDel15", DoubleType(), True),
    StructField("ArrivalDelayGroups", IntegerType(), True),
    StructField("ArrTimeBlk", StringType(), True),

    StructField("Cancelled", DoubleType(), True),
    StructField("CancellationCode", StringType(), True),
    StructField("Diverted", DoubleType(), True),

    StructField("CRSElapsedTime", DoubleType(), True),
    StructField("ActualElapsedTime", DoubleType(), True),
    StructField("AirTime", DoubleType(), True),
    StructField("Flights", DoubleType(), True),

    StructField("Distance", DoubleType(), True),
    StructField("DistanceGroup", IntegerType(), True),

    StructField("CarrierDelay", DoubleType(), True),
    StructField("WeatherDelay", DoubleType(), True),
    StructField("NASDelay", DoubleType(), True),
    StructField("SecurityDelay", DoubleType(), True),
    StructField("LateAircraftDelay", DoubleType(), True),

    StructField("FirstDepTime", IntegerType(), True),
    StructField("TotalAddGTime", DoubleType(), True),
    StructField("LongestAddGTime", DoubleType(), True),

    StructField("DivAirportLandings", DoubleType(), True),
    StructField("DivReachedDest", DoubleType(), True),
    StructField("DivActualElapsedTime", DoubleType(), True),
    StructField("DivArrDelay", DoubleType(), True),
    StructField("DivDistance", DoubleType(), True),

    StructField("Div1Airport", StringType(), True),
    StructField("Div1AirportID", IntegerType(), True),
    StructField("Div1AirportSeqID", IntegerType(), True),
    StructField("Div1WheelsOn", IntegerType(), True),
    StructField("Div1TotalGTime", DoubleType(), True),
    StructField("Div1LongestGTime", DoubleType(), True),
    StructField("Div1WheelsOff", IntegerType(), True),
    StructField("Div1TailNum", StringType(), True),

    StructField("Div2Airport", StringType(), True),
    StructField("Div2AirportID", IntegerType(), True),
    StructField("Div2AirportSeqID", IntegerType(), True),
    StructField("Div2WheelsOn", IntegerType(), True),
    StructField("Div2TotalGTime", DoubleType(), True),
    StructField("Div2LongestGTime", DoubleType(), True),
    StructField("Div2WheelsOff", IntegerType(), True),
    StructField("Div2TailNum", StringType(), True),

    StructField("Div3Airport", StringType(), True),
    StructField("Div3AirportID", IntegerType(), True),
    StructField("Div3AirportSeqID", IntegerType(), True),
    StructField("Div3WheelsOn", IntegerType(), True),
    StructField("Div3TotalGTime", DoubleType(), True),
    StructField("Div3LongestGTime", DoubleType(), True),
    StructField("Div3WheelsOff", IntegerType(), True),
    StructField("Div3TailNum", StringType(), True),

    StructField("Div4Airport", StringType(), True),
    StructField("Div4AirportID", IntegerType(), True),
    StructField("Div4AirportSeqID", IntegerType(), True),
    StructField("Div4WheelsOn", IntegerType(), True),
    StructField("Div4TotalGTime", DoubleType(), True),
    StructField("Div4LongestGTime", DoubleType(), True),
    StructField("Div4WheelsOff", IntegerType(), True),
    StructField("Div4TailNum", StringType(), True),

    StructField("Div5Airport", StringType(), True),
    StructField("Div5AirportID", IntegerType(), True),
    StructField("Div5AirportSeqID", IntegerType(), True),
    StructField("Div5WheelsOn", IntegerType(), True),
    StructField("Div5TotalGTime", DoubleType(), True),
    StructField("Div5LongestGTime", DoubleType(), True),
    StructField("Div5WheelsOff", IntegerType(), True),
    StructField("Div5TailNum", StringType(), True),

    StructField("_c109", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

# COMMAND ----------

# Now in this we make df_full dataframe in which we use flight_schema
df_full = spark.read.format("csv") \
    .option("header", "true") \
    .schema(flight_schema) \
    .load("s3://flight-analytics-mitansh/flight-data/*/*/*")

# COMMAND ----------

display(df_full)

# COMMAND ----------

# Now i want only 36 columns including _corrupt_record column out of 110 columns that i define in flight_schema.
cols_36 = [
    "FlightDate",
    "Reporting_Airline",
    "IATA_CODE_Reporting_Airline",
    "Tail_Number",
    "Flight_Number_Reporting_Airline",
    "Origin",
    "OriginCityName",
    "OriginState",
    "Dest",
    "DestCityName",
    "DestState",
    "CRSDepTime",
    "DepTime",
    "DepDelay",
    "DepDelayMinutes",
    "DepDel15",
    "DepartureDelayGroups",
    "CRSArrTime",
    "ArrTime",
    "ArrDelayMinutes",
    "ArrivalDelayGroups",
    "Cancelled",
    "CancellationCode",
    "Diverted",
    "CRSElapsedTime",
    "ActualElapsedTime",
    "AirTime",
    "Flights",
    "Distance",
    "DistanceGroup",
    "CarrierDelay",
    "WeatherDelay",
    "NASDelay",
    "SecurityDelay",
    "LateAircraftDelay",
    "_corrupt_record"
]

# COMMAND ----------

# In this i select specific columns that we need to work on that columns.
df = df_full.select(cols_36)

# COMMAND ----------

# Display content of 36 columns
display(df)

# COMMAND ----------

# In this we use permissive mode and use nullValue and columnNameOfCorruptRecord option to read data from corrupt_record column and use nullValue and emptyValue option to read data from corrupt_record column and use schema option to read data from corrupt_record.
df_full = spark.read.format("csv") \
    .option("header", "true") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .option("nullValue", "") \
    .option("emptyValue", "") \
    .schema(flight_schema) \
    .load("s3://flight-analytics-mitansh/flight-data/*/*/*")

# COMMAND ----------

display(df_full)

# COMMAND ----------

df = df_full.select(cols_36)

# COMMAND ----------

display(df)

# COMMAND ----------

# Create dictionary
cols_check = [
    "Distance",
    "DepDelay",
    "Reporting_Airline",
    "Origin",
    "Cancelled",
    "Dest"
]

null_dict = {c: 0 for c in cols_check}

# COMMAND ----------

total_count = df.count()
print("total number rows",total_count)

# COMMAND ----------

from pyspark.sql.functions import col

for c in null_dict.keys():
    null_count = df.filter(col(c).isNull()).count()
    null_dict[c] = (null_count / total_count) * 100

null_dict

# COMMAND ----------

cols_check = ["FlightDate", "Reporting_Airline", "Origin", "Dest"]

null_flag_dict = {c: f"{c}_null" for c in cols_check}

# COMMAND ----------

from pyspark.sql.functions import when, col

for c, new_col in null_flag_dict.items():
    df = df.withColumn(
        new_col,
        when(col(c).isNull(), 1).otherwise(0)
    )

# COMMAND ----------

display(null_flag_dict)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select(
    "FlightDate", "FlightDate_null",
    "Reporting_Airline", "Reporting_Airline_null",
    "Origin", "Origin_null",
    "Dest", "Dest_null"
).show(10)

# COMMAND ----------

from pyspark.sql.functions import when, col

df = df.withColumn(
    "dep_delay_flag",
    when((col("DepDelay") >= -120) & (col("DepDelay") <= 1440), 0).otherwise(1)
)

# COMMAND ----------

df = df.withColumn(
    "same_origin_destination",
    when(col("Origin") == col("Dest"), 1).otherwise(0)
)

# COMMAND ----------

df = df.withColumn(
    "is_domestic_flag",
    when((col("Distance") >= 1) & (col("Distance") <= 10000), 0).otherwise(1)
)

# COMMAND ----------

bronze_df = df

# COMMAND ----------

bronze_df.select(
    "DepDelay", "dep_delay_flag",
    "Origin", "Dest", "same_origin_destination",
    "Distance", "is_domestic_flag"
).show(10)

# COMMAND ----------

bronze_df = df
print("Total columns in bronze_df:", len(bronze_df.columns))
print("Columns:", bronze_df.columns)

# COMMAND ----------

from pyspark.sql.functions import year, month, to_date

bronze_df = bronze_df.withColumn("FlightDate", to_date("FlightDate")) \
       .withColumn("year", year("FlightDate")) \
       .withColumn("month", month("FlightDate"))

# COMMAND ----------

print("Total columns in bronze_df:", len(bronze_df.columns))

# COMMAND ----------

print("year" in bronze_df.columns)   # True aana chahiye
print("month" in bronze_df.columns)  # True aana chahiye

# COMMAND ----------

bronze_path = "s3a://flight-analytics-mitansh/bronze/"

bronze_df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .partitionBy("year", "month") \
  .save(bronze_path)

# COMMAND ----------

display(dbutils.fs.ls("s3a://flight-analytics-mitansh/bronze/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS flight_analytics;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE flight_analytics.bronze_flight
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM delta.`s3a://flight-analytics-mitansh/bronze/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM flight_analytics.bronze_flight LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM flight_analytics.bronze_flight;

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("code", StringType(), True),
    StructField("icao", StringType(), True),
    StructField("iata", StringType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("elevation", IntegerType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("tz", StringType(), True)
])

# COMMAND ----------

df = spark.read \
    .option("multiline", "true") \
    .schema(schema) \
    .json("s3://flight-analytics-mitansh/flight-data/airports_multiline.json")


df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
df_us = df.filter(col("country") == "US")

display(df_us)

# COMMAND ----------

delta_path = "s3://flight-analytics-mitansh/delta/Airport"

df_us.write.format("delta") \
    .mode("overwrite") \
    .save(delta_path)

# COMMAND ----------

df = spark.read.format("delta") \
    .load("s3://flight-analytics-mitansh/delta/Airport")

df.show(5)