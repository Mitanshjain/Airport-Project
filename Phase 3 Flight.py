# Databricks notebook source
# MAGIC %md
# MAGIC ## **Create a New Notebook and Access Delta Table using Same DataFrame df_silver**

# COMMAND ----------

# Load silver delta table from S3
df_silver = spark.read.format("delta") \
    .load("s3://flight-analytics-mitansh/mitansh-flight/flight/df_silver/")

# Verify
print("Total Rows   :", df_silver.count())
print("Total Columns:", len(df_silver.columns))
display(df_silver)

# COMMAND ----------

# Check schema and columns
df_silver.printSchema()
print("Columns:", df_silver.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Find the Time Required for Counting Number of Rows Without Caching**

# COMMAND ----------

import time

# Start timer - time shuru karo
start_time = time.time()

# Count rows
row_count = df_silver.count()

# End timer - time khatam karo
end_time = time.time()

# Time calculate karo
time_taken = (end_time - start_time, 2)

print("Total Rows :", row_count)
print("Time Taken :", time_taken, "seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Perform Caching on df_silver DataFrame using Temp View (cache/persist not supported in Serverless Free Edition)**

# COMMAND ----------

# Serverless Free Edition me cache nahi chalta
# Temp View use karo - same concept hai
df_silver.createOrReplaceTempView("df_silver_cached")

print("DataFrame cached successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **After Performing Caching Again Count the Number of Rows and Find the Time Required**

# COMMAND ----------

import time

# Start timer
start_time_cached = time.time()

# df_silver seedha count karo - second baar fast hoga
# Spark Photon Engine internally fast read karega
row_count_cached = df_silver.count()

# End timer
end_time_cached = time.time()

time_taken_cached = (end_time_cached - start_time_cached, 2)

print("Total Rows :", row_count_cached)
print("Time Taken :", time_taken_cached, "seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Compare Time Before and After Caching to Check Performance Improvement**

# COMMAND ----------

# Compare before and after caching
print("=" * 40)
print("Without Cache :", time_taken, "seconds")
print("With Cache    :", time_taken_cached, "seconds")
print("Time Saved    :", round(time_taken[0] - time_taken_cached, 2), "seconds")
print("=" * 40)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Create DataFrame from Silver Delta Table stored in S3**

# COMMAND ----------

df_silver = spark.read.format("delta") \
    .load("s3://flight-analytics-mitansh/mitansh-flight/flight/df_silver/")
    

display(df_silver)
df_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Group by Flight Year and Count Total Number of Flights (Test - Commented)**

# COMMAND ----------

# Test - Group by flight_year and count total flights
df_silver.groupBy("flight_year").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Group by Flight Year, Flight Month to Find Total Flights and Total Cancelled Flights using Sum (Test - Commented)**

# COMMAND ----------

# Test - Group by flight_year and flight_month
# Count total flights and sum of is_cancelled column

from pyspark.sql.functions import count, sum as spark_sum

df_silver.groupBy("flight_year", "flight_month") \
    .agg(
        count("*").alias("total_flights"),
        spark_sum("Cancelled").alias("total_cancelled")
    ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Flight Metrics Analysis (Delays, Cancellations, Diversions) by Year, Month, Airline Code, and Reporting Airline**

# COMMAND ----------

from pyspark.sql.functions import count, sum as spark_sum, avg, col, when, round

df_gold = df_silver.groupBy(
    "flight_year",
    "flight_month",
    "airline_code",
    "Reporting_Airline"
).agg(
    # Total number of flights
    count("*").alias("total_flights"),

    # Delayed flights using is_arrival_delayed
    spark_sum(
        when(col("is_arrival_delayed") == True, 1).otherwise(0)
    ).alias("delayed_flights"),

    # Total cancelled flights
    spark_sum(
        when(col("is_cancelled") == True, 1).otherwise(0)
    ).alias("total_flights_cancelled"),

    # Total delay minutes
    spark_sum("DepDelayMinutes").alias("total_delay_minutes"),

    # Total diverted flights
    spark_sum(
        when(col("is_diverted") == True, 1).otherwise(0)
    ).alias("total_diverted"),

    # Avg arrival delay - sirf wo flights jahan cancel nahi huyi
    round(
        avg(
            when(col("is_cancelled") == False, col("ArrDelayMinutes"))
        ), 5
    ).alias("avg_arr_delay_minutes")
)

display(df_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Create Median Arrival Delay Column for Non-Cancelled Flights using Percentile Approximate Function**

# COMMAND ----------

from pyspark.sql.functions import percentile_approx, col

# Create is_median_arrival_delay column
# percentile_approx with 0.5 = median of ArrDelayMinutes
df_gold = df_silver.groupBy(
    "flight_year",
    "flight_month",
    "airline_code",
    "Reporting_Airline"
).agg(
    percentile_approx(
        col("ArrDelayMinutes"), 0.5
    ).alias("is_median_arrival_delay")
)

display(df_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Filter and Count Valid Flights (Excluding Cancelled and Arrival Delayed Flights)**

# COMMAND ----------

from pyspark.sql.functions import col, when

# Test - check flights where both is_cancelled and is_arrival_delayed are False
# Matlab - flight cancel nahi huyi aur delay bhi nahi huyi

df_gold = df_silver.withColumn(
    "is_clean_flight",
    when(
        (col("is_cancelled") == False) & 
        (col("is_arrival_delayed") == False),
        "Not Cancelled and Not Delayed"
    ).otherwise("Cancelled or Delayed")
)

# Display result
df_gold.select(
    "is_cancelled",
    "is_arrival_delayed",
    "is_clean_flight"
).show(20, False)

# Count karo
clean_flights = df_gold.filter(
    col("is_clean_flight") == "Not Cancelled and Not Delayed"
).count()

print("Clean Flights (Not Cancelled + Not Delayed):", clean_flights)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Find On-Time Flights and Percentage for Non-Cancelled and Non-Delayed Flights**

# COMMAND ----------

from pyspark.sql.functions import count, sum as spark_sum, col, when, round

# On time flights from df_silver
# is_cancelled = False and is_arrival_delayed = False
df_gold = df_silver.groupBy(
    "flight_year",
    "flight_month",
    "airline_code",
    "Reporting_Airline"
).agg(
    # Total flights
    count("*").alias("total_flights"),

    # On time flights
    spark_sum(
        when(
            (col("is_cancelled") == False) &
            (col("is_arrival_delayed") == False), 1
        ).otherwise(0)
    ).alias("on_time_flights"),

    # On time percentage
    round(
        spark_sum(
            when(
                (col("is_cancelled") == False) &
                (col("is_arrival_delayed") == False), 1
            ).otherwise(0)
        ) / count("*") * 100, 2
    ).alias("on_time_flight_percentage")
)

display(df_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Find Cancelled Flight Percentage based on Total Flights**

# COMMAND ----------

from pyspark.sql.functions import count, sum as spark_sum, col, when, round

# Cancelled flight percentage
# Formula: cancelled flights / total flights * 100
df_gold = df_silver.groupBy(
    "flight_year",
    "flight_month",
    "airline_code",
    "Reporting_Airline"
).agg(
    # Total flights
    count("*").alias("total_flights"),

    # Total cancelled flights
    spark_sum(
        when(col("is_cancelled") == True, 1).otherwise(0)
    ).alias("total_flights_cancelled"),

    # Cancelled flight percentage
    round(
        spark_sum(
            when(col("is_cancelled") == True, 1).otherwise(0)
        ) / count("*") * 100, 2
    ).alias("cancelled_flight_percentage")
)

display(df_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Find Average Carrier Delay, Weather Delay, Security Delay and Late Aircraft Delay**

# COMMAND ----------

from pyspark.sql.functions import avg, round, col

# All avg delay metrics
df_gold = df_silver.groupBy(
    "flight_year",
    "flight_month",
    "airline_code",
    "Reporting_Airline"
).agg(
    # Avg carrier delay
    round(avg(col("CarrierDelay")), 2).alias("avg_carrier_delay"),

    # Avg weather delay
    round(avg(col("WeatherDelay")), 2).alias("avg_weather_delay"),

    # Avg security delay
    round(avg(col("SecurityDelay")), 2).alias("avg_security_delay"),

    # Avg late aircraft delay
    round(avg(col("LateAircraftDelay")), 2).alias("avg_late_aircraft_delay")
)

display(df_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC # **Find Average Distance Travelled and Total Distance Travelled**

# COMMAND ----------

from pyspark.sql.functions import avg, sum as spark_sum, round, col

# Distance metrics
df_gold = df_silver.groupBy(
    "flight_year",
    "flight_month",
    "airline_code",
    "Reporting_Airline"
).agg(
    # Average distance travelled
    round(avg(col("Distance")), 2).alias("avg_distance_travelled"),

    # Total distance travelled
    round(spark_sum(col("Distance")), 2).alias("total_distance_travelled")
)

display(df_gold)

# COMMAND ----------

from pyspark.sql.functions import col, when, count, sum, avg, round, percentile_approx
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

# Common grouping columns

group_cols = ["flight_year", "flight_month", "airline_code", "reporting_airline"]


# df_gold_1 : Basic Flight Metrics

df_gold_1 = df_silver.groupBy(*group_cols).agg(
    count("*").alias("total_number_of_flights"),
    sum(when(col("is_arrival_delayed") == True, 1).otherwise(0)).alias("total_delayed_flights"),
    sum(when(col("is_cancelled") == True, 1).otherwise(0)).alias("total_flights_cancelled"),
    sum(when(col("is_diverted") == True, 1).otherwise(0)).alias("total_flights_diverted")
)

# COMMAND ----------

# df_gold_2 : Average Arrival Delay (only non-cancelled flights)

df_gold_2 = df_silver.groupBy(*group_cols).agg(
    round(
        avg(
            when(col("is_cancelled") == False, col("ArrDelayMinutes"))
        ),
        4
    ).alias("avg_arrival_delay_minutes")
)

# COMMAND ----------

# df_gold_3 : On-time / Cancelled Percentage Metrics

df_on_time_base = df_silver.withColumn(
    "on_time_flight",
    when(
        (col("is_cancelled") == False) & (col("is_arrival_delayed") == False),
        1
    ).otherwise(0)
).withColumn(
    "non_cancelled_flight",
    when(col("is_cancelled") == False, 1).otherwise(0)
)

df_gold_3 = df_on_time_base.groupBy(*group_cols).agg(
    count("*").alias("total_flights"),
    sum("on_time_flight").alias("on_time_flights"),
    sum("non_cancelled_flight").alias("total_non_cancelled_flights")
).withColumn(
    "on_time_flight_percentage",
    round((col("on_time_flights") / col("total_flights")) * 100, 2)
).withColumn(
    "non_cancelled_flight_percentage",
    round((col("total_non_cancelled_flights") / col("total_flights")) * 100, 2)
).withColumn(
    "cancelled_flight_percentage",
    round(((col("total_flights") - col("total_non_cancelled_flights")) / col("total_flights")) * 100, 2)
).select(
    *group_cols,
    "on_time_flight_percentage",
    "non_cancelled_flight_percentage",
    "cancelled_flight_percentage"
)

# COMMAND ----------

# df_gold_4 : Median Arrival Delay (only non-cancelled flights)

df_gold_4 = df_silver.filter(col("is_cancelled") == False).groupBy(*group_cols).agg(
    percentile_approx("ArrDelayMinutes", 0.5).alias("median_arrival_delay")
)

# COMMAND ----------

# df_gold_5 : Delay + Distance Metrics

df_gold_5 = df_silver.groupBy(*group_cols).agg(
    round(avg("CarrierDelay"), 2).alias("avg_carrier_delay"),
    round(avg("WeatherDelay"), 2).alias("avg_weather_delay"),
    round(avg("SecurityDelay"), 2).alias("avg_security_delay"),
    round(avg("LateAircraftDelay"), 2).alias("avg_late_aircraft_delay"),
    round(avg("Distance"), 2).alias("avg_distance_travelled"),
    round(sum("Distance"), 2).alias("total_distance_travelled")
)

# COMMAND ----------

# Final GOLD dataframe by joining all KPI blocks

df_gold = df_gold_1 \
    .join(df_gold_2, on=group_cols, how="left") \
    .join(df_gold_3, on=group_cols, how="left") \
    .join(df_gold_4, on=group_cols, how="left") \
    .join(df_gold_5, on=group_cols, how="left")


# COMMAND ----------

# Create monthly_rank
# Partition by flight_year, flight_month
# Order by total_number_of_flights DESC

rank_window = Window.partitionBy("flight_year", "flight_month") \
                    .orderBy(col("total_number_of_flights").desc())

df_gold = df_gold.withColumn(
    "monthly_rank",
    dense_rank().over(rank_window)
)


# Final display

display(df_gold)


# Save as Delta Table
# Table Name = airline_api

df_gold.write.format("delta").mode("overwrite").save("s3://flight-analytics-mitansh/gold_layer/airline_api_1g")

# COMMAND ----------

from pyspark.sql.functions import col, when, count, sum, avg, round, concat_ws, countDistinct


# Step 1: Route column create karo
# route = origin_code + "-" + destination_code
# Example: JFK-LAX

df_route_base = df_silver.withColumn(
    "route",
    concat_ws("-", col("origin_code"), col("destination_code"))
)

# COMMAND ----------

# Step 2: On-time flight flag create karo
# Condition:
# 1 = flight cancelled nahi honi chahiye
# 2 = flight arrival delayed bhi nahi honi chahiye
# On-time = valid flight + no arrival delay

df_route_base = df_route_base.withColumn(
    "on_time_flight",
    when(
        (col("is_cancelled") == False) & (col("is_arrival_delayed") == False),
        1
    ).otherwise(0)
)

# COMMAND ----------


# Step 3: Group by route level KPI calculate karo
# Group by:
# - flight_year
# - route
# - origin_code
# - destination_code

df_gold_route_kpi = df_route_base.groupBy(
    "flight_year",
    "route",
    "origin_code",
    "destination_code"
).agg(
    # 1. Total number of flights on that route
    count("*").alias("number_of_flights"),

    # 2. Average arrival delay (only non-cancelled flights)
    round(
        avg(
            when(col("is_cancelled") == False, col("ArrDelayMinutes"))
        ),
        2
    ).alias("avg_arrival_delay"),

    # 3. Average distance travelled on that route
    round(
        avg("Distance"),
        2
    ).alias("avg_distance_travelled"),

    # 4. Total delayed flights on that route
    sum(
        when(col("is_arrival_delayed") == True, 1).otherwise(0)
    ).alias("total_delayed_flights"),

    # 5. Total on-time flights
    sum("on_time_flight").alias("total_on_time_flights"),

    # 6. Number of unique airlines on that route
    countDistinct("airline_code").alias("number_of_airlines_on_route")
)

# COMMAND ----------

# Step 4: On-time flight percentage calculate karo
# Formula:
# (total_on_time_flights / number_of_flights) * 100

df_gold_route_kpi = df_gold_route_kpi.withColumn(
    "on_time_percentage_airline_percentage",
    round(
        (col("total_on_time_flights") / col("number_of_flights")) * 100,
        2
    )
)

# COMMAND ----------

# Step 5: Optional cleanup
# Agar total_on_time_flights final table me nahi chahiye
# to drop kar do

df_gold_route_kpi = df_gold_route_kpi.drop("total_on_time_flights")


# Step 6: Final display

display(df_gold_route_kpi)


# Step 7: Save as Delta Table
# Table name = gold_route_kpi

df_gold_route_kpi.write.format("delta").mode("overwrite").save("s3://flight-analytics-mitansh/gold_layer/gold_route_KPI_2g")

# COMMAND ----------

df_silver = spark.read.format("delta").load("s3://flight-analytics-mitansh/mitansh-flight/flight/df_silver/")
airport_df=spark.read.format("delta").load("s3://flight-analytics-mitansh/delta/Airport/")
display(airport_df)

# COMMAND ----------

from pyspark.sql.functions import col
df_cl_join=df_silver.join(airport_df,
                          df_silver["Origin"]==airport_df["iata"],"inner")

# COMMAND ----------

from pyspark.sql.functions import col, when, count, sum, avg, round, concat_ws, lpad


# Step 1: Base dataframe create karo
# Departure on-time flag create karo
# Condition:
# flight cancelled nahi honi chahiye
# aur departure delayed bhi nahi honi chahiye

df_departure_base = df_cl_join.withColumn(
    "departure_on_time_flight",
    when(
        (col("is_cancelled") == False) & (col("is_departure_delayed") == False),
        1
    ).otherwise(0)
)


# COMMAND ----------

# Step 2: Group by departure airport level
# Group by:
# - flight_year
# - flight_month
# - origin_code
# - airport_name
# - airport_city
# - airport_state
# - longitude
# - latitude

df_gold_airport_departure_kpi = df_departure_base.groupBy(
    "flight_year",
    "flight_month",
    "origin_code",
    "name",
    "city",
    "state",
    "lon",
    "lat"
).agg(
    # 1. Total departures
    count("*").alias("total_departure"),

    # 2. Total cancelled departures
    sum(
        when(col("is_cancelled") == True, 1).otherwise(0)
    ).alias("total_cancelled_departure"),

    # 3. Average delayed departure (only non-cancelled flights)
    round(
        avg(
            when(col("is_cancelled") == False, col("DepDelayMinutes"))
        ),
        2
    ).alias("avg_delayed_departure"),

    # 4. Total on-time departures
    sum("departure_on_time_flight").alias("total_on_time_departure"),

    # 5. Average route distance
    round(
        avg("Distance"),
        2
    ).alias("avg_route_distance"),

    # 6. Number of flights operating for that departure airport
    count("*").alias("number_of_flights_operating"),

    # 7. Average airtime from that departure airport
    round(
        avg(
            when(col("is_cancelled") == False, col("AirTime"))
        ),
        2
    ).alias("avg_airtime")
)

# COMMAND ----------

# Step 3: Departure on-time percentage calculate karo
# Formula:
# (total_on_time_departure / total_departure) * 100

df_gold_airport_departure_kpi = df_gold_airport_departure_kpi.withColumn(
    "departure_on_time_percentage",
    round(
        (col("total_on_time_departure") / col("total_departure")) * 100,
        2
    )
)

# COMMAND ----------

# Step 4: Year-Month column create karo
# Format:
# flight_year-flight_month
# Example:
# 2024-1

df_gold_airport_departure_kpi = df_gold_airport_departure_kpi.withColumn(
    "year_month",
    concat_ws("-", col("flight_year"), lpad(col("flight_month"),2, "0")
)
)

# COMMAND ----------

# Step 5: Helper column drop karo
# total_on_time_departure sirf percentage ke liye use hui thi

df_gold_airport_departure_kpi = df_gold_airport_departure_kpi.drop("total_on_time_departure")


# Step 6: Final display

display(df_gold_airport_departure_kpi)


# Step 7: Save as Delta Table
# Table name = gold_airport_departure_kpi

df_gold_airport_departure_kpi.write.format("delta").mode("overwrite").save("s3://flight-analytics-mitansh/gold_layer/gold_airport_departure_kpi_3g")


# COMMAND ----------

from pyspark.sql.functions import col, sum, round


# Step 1: Filter only valid delayed flights
# Conditions:
# 1. Flight cancelled nahi honi chahiye
# 2. Arrival delay minutes > 0 hone chahiye

df_delay_base = df_silver.filter(
    (col("is_cancelled") == False) & (col("ArrDelayMinutes") > 0)
)

# COMMAND ----------

 # Step 2: Group by year, month, airline_code

df_delay_cause_table = df_delay_base.groupBy(
    "flight_year",
    "flight_month",
    "airline_code"
).agg(
    # 1. Total arrival delay minutes
    round(
        sum("ArrDelayMinutes"),
        2
    ).alias("total_minutes_delayed"),

    # 2. Total weather delay minutes
    round(
        sum("WeatherDelay"),
        2
    ).alias("total_weather_delayed_minutes"),

    # 3. Total carrier delay minutes
    round(
        sum("CarrierDelay"),
        2
    ).alias("total_carrier_delayed_minutes"),

    # 4. Total security delay minutes
    round(
        sum("SecurityDelay"),
        2
    ).alias("total_security_delayed_minutes"),

    # 5. Total late aircraft delay minutes
    round(
        sum("LateAircraftDelay"),
        2
    ).alias("total_late_aircraft_delayed_minutes")
)

# COMMAND ----------

# Step 3: Percentage calculation for each delay cause
# Formula:
# (cause_delay_minutes / total_minutes_delayed) * 100

df_delay_cause_table = df_delay_cause_table.withColumn(
    "weather_delay_percentage",
    round(
        (col("total_weather_delayed_minutes") / col("total_minutes_delayed")) * 100,
        2
    )
).withColumn(
    "carrier_delay_percentage",
    round(
        (col("total_carrier_delayed_minutes") / col("total_minutes_delayed")) * 100,
        2
    )
).withColumn(
    "security_delay_percentage",
    round(
        (col("total_security_delayed_minutes") / col("total_minutes_delayed")) * 100,
        2
    )
).withColumn(
    "late_aircraft_delay_percentage",
    round(
        (col("total_late_aircraft_delayed_minutes") / col("total_minutes_delayed")) * 100,
        2
    )
)

# COMMAND ----------

# Step 4: Final display

display(df_delay_cause_table)


# Step 5: Save as Delta Table
# Table name = delay_cause_table

df_delay_cause_table.write.format("delta").mode("overwrite").save("s3://flight-analytics-mitansh/gold_layer/delay_cause_table_4g")