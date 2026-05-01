# Databricks notebook source
# DBTITLE 1,Cell 1
# Read file from external source like s3 to databricks
# IN this we read only one year like 2021 and month is 1
# We use inferschema for just checking the printschema that what data types of columns so that can we apply struct type and struct field.

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Define schema manually
schema = StructType([
    StructField("year", IntegerType(), True),
    StructField("Quarter", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("DayofMonth", IntegerType(), True),
    StructField("DayOfWeek", IntegerType(), True),
    StructField("FlightDate", DateType(), True),
    StructField("Reporting_Airline", StringType(), True),
    StructField("DOT_ID_Reporting_Airline", IntegerType(), True),
    StructField("IATA_CODE_Reporting_Airline", StringType(), True),
    StructField("Tail_Number", StringType(), True),
    StructField("Flight_Number_Reporting_Airline", IntegerType(), True),
    StructField("OriginAirportID", IntegerType(), True),
    StructField("OriginAirportSeqID", IntegerType(), True),
    StructField("OriginCityMarketID", IntegerType(), True),
    StructField("Origin", StringType(), True),
    StructField("OriginCityName", StringType(), True),
    StructField("OriginState", StringType(), True),
    StructField("OriginStateFips", IntegerType(), True),
    StructField("OriginStateName", StringType(), True),
    StructField("OriginWac", IntegerType(), True),
    StructField("DestAirportID", IntegerType(), True),
    StructField("DestAirportSeqID", IntegerType(), True),
    StructField("DestCityMarketID", IntegerType(), True),
    StructField("Dest", StringType(), True),
    StructField("DestCityName", StringType(), True),
    StructField("DestState", StringType(), True),
    StructField("DestStateFips", IntegerType(), True),
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
    StructField("DivAirportLandings", IntegerType(), True),
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
    StructField("Div3AirportID", StringType(), True),
    StructField("Div3AirportSeqID", StringType(), True),
    StructField("Div3WheelsOn", StringType(), True),
    StructField("Div3TotalGTime", StringType(), True),
    StructField("Div3LongestGTime", StringType(), True),
    StructField("Div3WheelsOff", StringType(), True),
    StructField("Div3TailNum", StringType(), True),
    StructField("Div4Airport", StringType(), True),
    StructField("Div4AirportID", StringType(), True),
    StructField("Div4AirportSeqID", StringType(), True),
    StructField("Div4WheelsOn", StringType(), True),
    StructField("Div4TotalGTime", StringType(), True),
    StructField("Div4LongestGTime", StringType(), True),
    StructField("Div4WheelsOff", StringType(), True),
    StructField("Div4TailNum", StringType(), True),
    StructField("Div5Airport", StringType(), True),
    StructField("Div5AirportID", StringType(), True),
    StructField("Div5AirportSeqID", StringType(), True),
    StructField("Div5WheelsOn", StringType(), True),
    StructField("Div5TotalGTime", StringType(), True),
    StructField("Div5LongestGTime", StringType(), True),
    StructField("Div5WheelsOff", StringType(), True),
    StructField("Div5TailNum", StringType(), True),
    StructField("_c109", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

sample_df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("s3://flight-analytics-mitansh/flight-data/2021/") \
    .limit(10)

sample_df.display()

# COMMAND ----------

sample_df.printSchema()

# COMMAND ----------

schema = sample_df.schema
print(schema)

# COMMAND ----------



from pyspark.sql.types import *

schema = StructType([

    StructField("year", IntegerType(), True),
    StructField("Quarter", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("DayofMonth", IntegerType(), True),
    StructField("DayOfWeek", IntegerType(), True),
    StructField("FlightDate", DateType(), True),

    StructField("Reporting_Airline", StringType(), True),
    StructField("DOT_ID_Reporting_Airline", IntegerType(), True),
    StructField("IATA_CODE_Reporting_Airline", StringType(), True),
    StructField("Tail_Number", StringType(), True),
    StructField("Flight_Number_Reporting_Airline", IntegerType(), True),

    StructField("OriginAirportID", IntegerType(), True),
    StructField("OriginAirportSeqID", IntegerType(), True),
    StructField("OriginCityMarketID", IntegerType(), True),
    StructField("Origin", StringType(), True),
    StructField("OriginCityName", StringType(), True),
    StructField("OriginState", StringType(), True),
    StructField("OriginStateFips", IntegerType(), True),
    StructField("OriginStateName", StringType(), True),
    StructField("OriginWac", IntegerType(), True),

    StructField("DestAirportID", IntegerType(), True),
    StructField("DestAirportSeqID", IntegerType(), True),
    StructField("DestCityMarketID", IntegerType(), True),
    StructField("Dest", StringType(), True),
    StructField("DestCityName", StringType(), True),
    StructField("DestState", StringType(), True),
    StructField("DestStateFips", IntegerType(), True),
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
    StructField("DivAirportLandings", IntegerType(), True),

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
    StructField("Div3AirportID", StringType(), True),
    StructField("Div3AirportSeqID", StringType(), True),
    StructField("Div3WheelsOn", StringType(), True),
    StructField("Div3TotalGTime", StringType(), True),
    StructField("Div3LongestGTime", StringType(), True),
    StructField("Div3WheelsOff", StringType(), True),
    StructField("Div3TailNum", StringType(), True),

    StructField("Div4Airport", StringType(), True),
    StructField("Div4AirportID", StringType(), True),
    StructField("Div4AirportSeqID", StringType(), True),
    StructField("Div4WheelsOn", StringType(), True),
    StructField("Div4TotalGTime", StringType(), True),
    StructField("Div4LongestGTime", StringType(), True),
    StructField("Div4WheelsOff", StringType(), True),
    StructField("Div4TailNum", StringType(), True),

    StructField("Div5Airport", StringType(), True),
    StructField("Div5AirportID", StringType(), True),
    StructField("Div5AirportSeqID", StringType(), True),
    StructField("Div5WheelsOn", StringType(), True),
    StructField("Div5TotalGTime", StringType(), True),
    StructField("Div5LongestGTime", StringType(), True),
    StructField("Div5WheelsOff", StringType(), True),
    StructField("Div5TailNum", StringType(), True),

    StructField("_c109", StringType(), True),

    #  Add corrupt column
    StructField("_corrupt_record", StringType(), True)
])

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
schema = StructType([
    StructField(field.name, StringType(), True)
    for field in schema.fields
])

# COMMAND ----------

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv("s3://flight-analytics-mitansh/2021/Month_01/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2021_1.csv")

df.display()

# COMMAND ----------

df.count()

# COMMAND ----------

df_2021 = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv("s3://flight-analytics-mitansh/2021/")

# COMMAND ----------

df_2022 = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv("s3://flight-analytics-mitansh/2022/")

# COMMAND ----------

df_clean = spark.read \
    .option("header", "true") \
    .option("nullValue", "null") \
    .option("treatEmptyValuesAsNulls", "true") \
    .schema(schema) \
    .csv("s3://flight-analytics-mitansh/2021")

# COMMAND ----------

df_clean2 = spark.read \
    .option("header", "true") \
    .option("nullValue", "null") \
    .option("treatEmptyValuesAsNulls", "true") \
    .schema(schema) \
    .csv("s3://flight-analytics-mitansh/2022/")

# COMMAND ----------

df_clean.display()

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([

    StructField("Year", IntegerType(), True),
    StructField("Quarter", IntegerType(), True),
    StructField("Month", IntegerType(), True),
    StructField("DayofMonth", IntegerType(), True),
    StructField("DayOfWeek", IntegerType(), True),
    StructField("FlightDate", DateType(), True),

    StructField("Reporting_Airline", StringType(), True),
    StructField("DOT_ID_Reporting_Airline", IntegerType(), True),
    StructField("IATA_CODE_Reporting_Airline", StringType(), True),
    StructField("Tail_Number", StringType(), True),
    StructField("Flight_Number_Reporting_Airline", IntegerType(), True),

    StructField("OriginAirportID", IntegerType(), True),
    StructField("OriginAirportSeqID", IntegerType(), True),
    StructField("OriginCityMarketID", IntegerType(), True),
    StructField("Origin", StringType(), True),
    StructField("OriginCityName", StringType(), True),
    StructField("OriginState", StringType(), True),
    StructField("OriginStateFips", IntegerType(), True),
    StructField("OriginStateName", StringType(), True),
    StructField("OriginWac", IntegerType(), True),

    StructField("DestAirportID", IntegerType(), True),
    StructField("DestAirportSeqID", IntegerType(), True),
    StructField("DestCityMarketID", IntegerType(), True),
    StructField("Dest", StringType(), True),
    StructField("DestCityName", StringType(), True),
    StructField("DestState", StringType(), True),
    StructField("DestStateFips", IntegerType(), True),
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

    StructField("DivAirportLandings", IntegerType(), True),
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
    StructField("Div3AirportID", StringType(), True),
    StructField("Div3AirportSeqID", StringType(), True),
    StructField("Div3WheelsOn", StringType(), True),
    StructField("Div3TotalGTime", StringType(), True),
    StructField("Div3LongestGTime", StringType(), True),
    StructField("Div3WheelsOff", StringType(), True),
    StructField("Div3TailNum", StringType(), True),

    StructField("Div4Airport", StringType(), True),
    StructField("Div4AirportID", StringType(), True),
    StructField("Div4AirportSeqID", StringType(), True),
    StructField("Div4WheelsOn", StringType(), True),
    StructField("Div4TotalGTime", StringType(), True),
    StructField("Div4LongestGTime", StringType(), True),
    StructField("Div4WheelsOff", StringType(), True),
    StructField("Div4TailNum", StringType(), True),

    StructField("Div5Airport", StringType(), True),
    StructField("Div5AirportID", StringType(), True),
    StructField("Div5AirportSeqID", StringType(), True),
    StructField("Div5WheelsOn", StringType(), True),
    StructField("Div5TotalGTime", StringType(), True),
    StructField("Div5LongestGTime", StringType(), True),
    StructField("Div5WheelsOff", StringType(), True),
    StructField("Div5TailNum", StringType(), True),

    StructField("_c109", StringType(), True),
    StructField("_corrupt_record", StringType(), True)

])

# COMMAND ----------

df = spark.read.option("recursiveFileLookup", "true").csv(
    "s3a://flight-analytics-mitansh/",
    header=True,
    schema=schema
)

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

df.inputFiles()

# COMMAND ----------

df.select("Year", "Month").distinct().show()

# COMMAND ----------

df_2021 = spark.read.option("recursiveFileLookup", "true").csv(
    "s3a://flight-analytics-mitansh/2021/",
    header=True,
    schema=schema
)

# COMMAND ----------

df_2021.count()

# COMMAND ----------

df_2022 = spark.read.option("recursiveFileLookup", "true").csv(
    "s3a://flight-analytics-mitansh/2022/",
    header=True,
    schema=schema
)

# COMMAND ----------

df_2022.count()

# COMMAND ----------

df_clean = spark.read.format("csv") \
    .option("header", "true") \
    .option("nullValue", None) \
    .option("emptyValue", None) \
    .option("recursiveFileLookup", "true") \
    .schema(schema) \
    .load("s3a://flight-analytics-mitansh/")

# COMMAND ----------

display(df_clean.select("DepDelay"))

# COMMAND ----------

display(df)