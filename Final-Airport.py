# Databricks notebook source
# We use inferschema for just checking the printschema that what data types of columns so that can we apply struct type and struct field.
df = spark.read.format("csv") \
    .option("header", "true") \
            .load("s3://flight-analytics-mitansh/flight-data/*/*/*")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

df.dtypes # this gives column name and exact data types

# COMMAND ----------

df.columns 

# COMMAND ----------

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

    StructField("_c109", StringType(), True)
])

# COMMAND ----------

# Create DataFrame Using This Schema(flight_schema)

df_schema = spark.read.format("csv") \
    .option("header", "true") \
    .schema(flight_schema) \
    .load("s3://flight-analytics-mitansh/flight-data/*/*/*")

# COMMAND ----------

# Observe the Issue => Some values become null => Reason => You defined it as IntegerType => Actual data is string
df_schema.select("Flight_Number_Reporting_Airline").show()

# COMMAND ----------

# In above column we didnt detrmine the null values for proving that and verify this we use this 
from pyspark.sql.functions import col

df_schema.filter(
    col("Flight_Number_Reporting_Airline").isNull()
).count()
# so there is no null value 

# COMMAND ----------

# Partition Reading
df_2021 = spark.read.format("csv") \
    .option("header", "true") \
    .load("s3://flight-analytics-mitansh/flight-data/2021/*/*")

# COMMAND ----------

# Read BOTH Years (Partition Comparison) => (2021,2022)
df_all = spark.read.format("csv") \
    .option("header", "true") \
    .load("s3://flight-analytics-mitansh/flight-data/*/*/*")

# COMMAND ----------

# Verify Difference
df_2021.count()

# COMMAND ----------

df_all.count()

# COMMAND ----------

# NULL VALUE Handling
df_null = spark.read.format("csv") \
    .option("header", "true") \
    .option("nullValue", "NA") \
    .load("s3://flight-analytics-mitansh/flight-data/*/*/*")

# COMMAND ----------

display(df_null)

# COMMAND ----------

# Corrupt Record Column
from pyspark.sql.functions import when, col, concat_ws

df_custom = df_schema.withColumn(
    "_corrupt_record",
    when(
        col("Flight_Number_Reporting_Airline").isNull(),
        concat_ws(",", *df_schema.columns)
    ).otherwise(None)
)
# We are taking specific column that named as "Flight_Number_Reporting_Airline" 

# COMMAND ----------

df_custom.columns

# COMMAND ----------

df_custom.select("_corrupt_record").show(truncate=False)

# COMMAND ----------

display(df_custom) # By displaying we get null values in corrupt_record column

# COMMAND ----------

# we ckeck corrupt record column by taking multiple column from dataset 
from pyspark.sql.functions import when, col, concat_ws

df_custom1 = df_schema.withColumn(
    "_corrupt_record_new",
    when(
        col("Flight_Number_Reporting_Airline").isNull() |
        col("FlightDate").isNull() |
        col("Distance").isNull() |
        col("ArrDelay").isNull(),
        concat_ws(",", *df_schema.columns)
    ).otherwise(None)
)

# COMMAND ----------

display(df_custom1) # 2865 row get corrupt data

# COMMAND ----------


df_custom1.filter(col("_corrupt_record_new").isNotNull()).count()
# Now by checking multiple columns, Spark found rows with nulls in Distance, ArrDelay or FlightDate, so count = 96898.

# COMMAND ----------

# Create DataFrame using all options (header, null value, empty value, schema) with mode
# Spark allows three modes when reading CSVs:-
# PERMISSIVE :- default, tries to read bad rows and put them in _corrupt_record.
# DROPMALFORMED :- drops rows that cannot be parsed.
# FAILFAST :- fails immediately if a row cannot be parsed.

# COMMAND ----------

# We will read the same CSV 5th time according to your assignment:-

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col

df_mode_permissive = spark.read.format("csv") \
    .option("header", "true") \
    .option("nullValue", "NA") \
    .option("emptyValue", "") \
    .option("mode", "PERMISSIVE") \
    .schema(flight_schema) \
    .load("s3://flight-analytics-mitansh/flight-data/*/*/*")

# COMMAND ----------

df_mode_permissive.show(truncate=False)

# COMMAND ----------

display(df_mode_permissive)

# COMMAND ----------

total_rows = df_mode_permissive.count()
print("Total rows:", total_rows)

# COMMAND ----------

df_mode_drop = spark.read.format("csv") \
    .option("header", "true") \
    .option("nullValue", "NA") \
    .option("emptyValue", "") \
    .option("mode", "DROPMALFORMED") \
    .schema(flight_schema) \
    .load("s3://flight-analytics-mitansh/flight-data/*/*/*")

# COMMAND ----------

total_rows = df_mode_drop.count()
print("Total rows:", total_rows)

# COMMAND ----------

df_mode_failfast = spark.read.format("csv") \
    .option("header", "true") \
    .option("nullValue", "NA") \
    .option("emptyValue", "") \
    .option("mode", "FAILFAST") \
    .schema(flight_schema) \
    .load("s3://flight-analytics-mitansh/flight-data/*/*/*")

# COMMAND ----------

total_rows = df_mode_failfast.count()
print("Total rows:", total_rows)

# COMMAND ----------

# Create final DataFrame without schema
df_final = spark.read.format("csv") \
    .option("header", "true") \
    .option("nullValue", "NA") \
    .option("emptyValue", "") \
    .option("mode", "PERMISSIVE") \
    .load("s3://flight-analytics-mitansh/flight-data/*/*/*")

# COMMAND ----------

# Add metadata columns to the Bronze DataFrame
from pyspark.sql.functions import current_timestamp, col

df_bronze = df_final.withColumn("ingestion_time", current_timestamp()) \
    .withColumn("source_file", col("_metadata.file_path"))

# You need to add:-
# Current ingestion timestamp
# Source file name
# _corrupt_record_new

# COMMAND ----------

total_count = df_bronze.count()
print("Total records:", total_count)

# COMMAND ----------

# Now calculate the null value in flight date column.
from pyspark.sql.functions import col

flightdate_null = df_bronze.filter(
    col("FlightDate").isNull() | (col("FlightDate") == "")
).count()

flightdate_percent = (flightdate_null / total_count) * 100

print("FlightDate null %:", flightdate_percent)

# COMMAND ----------

# Now calculate the null value in Distance column.
distance_null = df_bronze.filter(
    col("Distance").isNull() | (col("Distance") == "")
).count()

distance_percent = (distance_null / total_count) * 100

print("Distance null %:", distance_percent)

# COMMAND ----------

# Now calculate the null calue in ArrDelay column
arrdelay_null = df_bronze.filter(
    col("ArrDelay").isNull() | (col("ArrDelay") == "")
).count()

arrdelay_percent = (arrdelay_null / total_count) * 100

print("ArrDelay null %:", arrdelay_percent)

# COMMAND ----------

# Now calculate the null value in DepDelay column.
depdelay_null = df_bronze.filter(
    col("DepDelay").isNull() | (col("DepDelay") == "")
).count()

depdelay_percent = (depdelay_null / total_count) * 100

print("DepDelay null %:", depdelay_percent)

# COMMAND ----------

# Now calculate the null value in Reporting_Airline column
airline_null = df_bronze.filter(
    col("Reporting_Airline").isNull() | (col("Reporting_Airline") == "")
).count()

airline_percent = (airline_null / total_count) * 100

print("Reporting_Airline null %:", airline_percent)

# COMMAND ----------

# flight date column validity check (max date - min date).
from pyspark.sql.functions import min, max

df_bronze.select(
    min("FlightDate").alias("min_date"),
    max("FlightDate").alias("max_date")
).show()