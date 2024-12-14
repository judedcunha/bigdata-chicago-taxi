-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Dataset location
-- MAGIC https://data.cityofchicago.org/Transportation/Taxi-Trips-2013-2023-/wrvz-psew/
-- MAGIC
-- MAGIC OR 
-- MAGIC
-- MAGIC https://data.cityofchicago.org/Transportation/Taxi-Trips-2024-/ajtu-isnz/
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ##### Google Drive Link
-- MAGIC https://drive.google.com/file/d/1wGpC1pMRH5hc-Ige8JAKX-feV9noJ3oA/view?usp=sharing

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Upload Dataset locally: 
-- MAGIC
-- MAGIC File > Create a Table, then upload. After that copy file path after uploading. And use it below.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Data ingestion

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import requests
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC import json
-- MAGIC
-- MAGIC # Create a Spark session
-- MAGIC spark = SparkSession.builder.getOrCreate()
-- MAGIC
-- MAGIC # Define API endpoint and parameters
-- MAGIC api_url = "https://data.cityofchicago.org/resource/ajtu-isnz.json"
-- MAGIC params = {
-- MAGIC     # "$where": "trip_start_timestamp between '2024-01-01T00:00:00' and '2024-01-31T23:59:59'",
-- MAGIC     "$limit": 4837195, 
-- MAGIC     "$offset": 0
-- MAGIC }
-- MAGIC
-- MAGIC # Fetch data from API
-- MAGIC response = requests.get(api_url, params=params)
-- MAGIC data = response.json()
-- MAGIC
-- MAGIC # Convert the JSON data to an RDD and then to a DataFrame
-- MAGIC rdd = spark.sparkContext.parallelize([json.dumps(record) for record in data])
-- MAGIC df = spark.read.json(rdd)
-- MAGIC
-- MAGIC # Display the data in the DataFrame
-- MAGIC df.show()
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from functools import reduce
-- MAGIC
-- MAGIC # Get the data from the local storage space
-- MAGIC df = spark.read.csv("/FileStore/tables/Taxi_Trips__2024___20241019.csv", header="true", inferSchema="true")
-- MAGIC oldColumns=df.columns
-- MAGIC newColumns =['trip_id',
-- MAGIC 'taxi_id',
-- MAGIC 'trip_start_timestamp',
-- MAGIC 'trip_end_timestamp',
-- MAGIC 'trip_seconds',
-- MAGIC 'trip_miles',
-- MAGIC 'pickup_census_tract',	
-- MAGIC 'dropoff_census_tract',
-- MAGIC 'pickup_community_area',
-- MAGIC 'dropoff_community_area',
-- MAGIC 'fare',
-- MAGIC 'tips',
-- MAGIC 'tolls',
-- MAGIC 'extras',
-- MAGIC 'trip_total',
-- MAGIC 'payment_type',
-- MAGIC 'company',
-- MAGIC 'pickup_centroid_latitude',
-- MAGIC 'pickup_centroid_longitude',
-- MAGIC 'pickup_centroid_location',
-- MAGIC 'dropoff_centroid_latitude',
-- MAGIC 'dropoff_centroid_longitude',
-- MAGIC 'dropoff_centroid_location'];
-- MAGIC
-- MAGIC # Rename the columns to make it easier to use it ahead
-- MAGIC df = reduce(lambda df, idx: df.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), df)
-- MAGIC df.columns

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Drop unnecessary columns
-- MAGIC df_cleaned = df.drop('trip_id', 'taxi_id', 
-- MAGIC                      'pickup_census_tract', 'dropoff_census_tract'
-- MAGIC                      )
-- MAGIC
-- MAGIC # Change null values to it's apppropiate meaning
-- MAGIC # df_cleaned['pickup_community_area'] = df_cleaned['pickup_community_area'].fillna("Outside Chicago")
-- MAGIC # df_cleaned['dropoff_community_area'] = df_cleaned['dropoff_community_area'].fillna("Outside Chicago")
-- MAGIC
-- MAGIC # Show the cleaned data
-- MAGIC df_cleaned.show(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.functions import col, date_format
-- MAGIC # Convert the string timestamp to timestamp type
-- MAGIC df_new = df_cleaned.withColumn('trip_start_timestamp', col('trip_start_timestamp').cast('timestamp')).withColumn('trip_end_timestamp', col('trip_end_timestamp').cast('timestamp'))
-- MAGIC
-- MAGIC # Format the timestamp to 'MM/dd/yyyy hh:mm:ss a'
-- MAGIC # df_2 = df_new.withColumn('formatted_timestamp', date_format(col('trip_start_timestamp'), 'MM/dd/yyyy hh:mm:ss a')).withColumn('formatted_end_timestamp', date_format(col('trip_end_timestamp'), 'MM/dd/yyyy hh:mm:ss a'))
-- MAGIC
-- MAGIC # # Show the resulting DataFrame
-- MAGIC # df_2.show(truncate=False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.functions import col, date_format
-- MAGIC # Convert the string timestamp to timestamp type
-- MAGIC df_new = df_cleaned.withColumn('trip_start_timestamp', col('trip_start_timestamp').cast('timestamp')).withColumn('trip_end_timestamp', col('trip_end_timestamp').cast('timestamp'))
-- MAGIC
-- MAGIC # Show the resulting DataFrame
-- MAGIC df_new.show(truncate=False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC
-- MAGIC # Required only if Date needs to be formated
-- MAGIC # df_cleaned=df_2
-- MAGIC
-- MAGIC # Create Company Dimension Table
-- MAGIC company_dim = df_cleaned.select("company").distinct()
-- MAGIC
-- MAGIC # Add surrogate key for Company Dimension table
-- MAGIC company_dim = company_dim.withColumn("company_id", F.monotonically_increasing_id())
-- MAGIC
-- MAGIC # Write the Company Dimension table to DBFS as Parquet
-- MAGIC company_dim.write.mode('overwrite').parquet("/dbfs/FileStore/tables/company_dim.parquet")
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Create Transaction Details Table
-- MAGIC transaction_fact = df_cleaned.select("fare", "tips", "tolls", "extras", "payment_type").distinct()
-- MAGIC
-- MAGIC # Add surrogate key for Transaction table
-- MAGIC transaction_fact = transaction_fact.withColumn("transaction_id", F.monotonically_increasing_id())
-- MAGIC
-- MAGIC # Write the Transaction Fact table to DBFS as Parquet
-- MAGIC transaction_fact.write.mode('overwrite').parquet("/dbfs/FileStore/tables/transaction_fact.parquet")
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Join with company_dim and transaction_fact to create foreign key relationships
-- MAGIC df_2=df_cleaned
-- MAGIC trip_fact = df_2.join(company_dim, "company") \
-- MAGIC               .join(transaction_fact, ["fare", "tips", "tolls", "extras", "payment_type"])
-- MAGIC
-- MAGIC # Select relevant columns for Trip Fact table
-- MAGIC trip_fact = trip_fact.select("trip_start_timestamp", "trip_end_timestamp", "trip_miles", 
-- MAGIC                              "company_id", "transaction_id","payment_type","pickup_centroid_latitude", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude","pickup_community_area","dropoff_community_area","trip_seconds")
-- MAGIC                              
-- MAGIC # Null values in the community areas indicate that the location is outside Chicago
-- MAGIC trip_fact = trip_fact.fillna({'pickup_community_area': 0, 'dropoff_community_area': 0})
-- MAGIC # Write the Trip Fact table to DBFS as Parquet
-- MAGIC trip_fact.write.mode('overwrite').parquet("/dbfs/FileStore/tables/trip_fact.parquet")
-- MAGIC trip_fact.show()

-- COMMAND ----------

-- Create Company Information Table
DROP TABLE IF EXISTS company_dim;
CREATE TABLE company_dim USING parquet OPTIONS (
  path "/dbfs/FileStore/tables/company_dim.parquet"
);
-- Verify Company Dimension table
SELECT
  *
FROM
  company_dim;

-- COMMAND ----------

-- Create Transaction Details Table
DROP TABLE IF EXISTS transaction_fact;
CREATE TABLE transaction_fact USING parquet OPTIONS (
  path "/dbfs/FileStore/tables/transaction_fact.parquet"
);
-- Verify Transaction Details table
SELECT
  *
FROM
  transaction_fact;

-- COMMAND ----------

-- Create Trip Details Table
DROP TABLE IF EXISTS trip_fact;
CREATE TABLE trip_fact USING parquet OPTIONS (path "/dbfs/FileStore/tables/trip_fact.parquet");
-- Verify Trip Details table
SELECT
  *
FROM
  trip_fact;

-- COMMAND ----------

SELECT
  t.trip_start_timestamp,
  t.trip_end_timestamp,
  t.trip_miles,
  c.company,
  f.fare,
  f.tips,
  f.tolls,
  f.extras,
  f.payment_type
FROM
  trip_fact t
  JOIN company_dim c ON t.company_id = c.company_id
  JOIN transaction_fact f ON t.transaction_id = f.transaction_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Initial Data exploration 

-- COMMAND ----------

SELECT
  count(*)
from
  trip_fact

-- COMMAND ----------

SELECT
  payment_type,
  avg(Fare) AS price
FROM
  transaction_fact
GROUP BY
  payment_type
ORDER BY
  payment_type

-- COMMAND ----------

WITH trips_by_day AS (
  SELECT
    to_date(
      to_timestamp(trip_start_timestamp, 'MM/dd/yyyy hh:mm:ss a')
    ) AS trip_date,
    COUNT(*) as num_trips
  FROM
    trip_fact
  WHERE
    to_date(
      to_timestamp(trip_start_timestamp, 'MM/dd/yyyy hh:mm:ss a')
    ) >= '2024-01-01'
    AND to_date(
      to_timestamp(trip_start_timestamp, 'MM/dd/yyyy hh:mm:ss a')
    ) < '2024-04-12'
  GROUP BY
    trip_date
  ORDER BY
    trip_date
)
SELECT
  trip_date,
  AVG(num_trips) OVER (
    ORDER BY
      trip_date ROWS BETWEEN 3 PRECEDING
      AND 3 FOLLOWING
  ) AS avg_num_trips
FROM
  trips_by_day

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We see that the number of trips keeps peaking during long weekends/holidays, and keeps gradually increasing.

-- COMMAND ----------

-- Descriptive statistics for fare, trip distance, and trip duration
SELECT
  MIN(fare) AS min_fare,
  MAX(fare) AS max_fare,
  AVG(fare) AS avg_fare,
  MIN(trip_miles) AS min_distance,
  MAX(trip_miles) AS max_distance,
  AVG(trip_miles) AS avg_distance,
  MIN(trip_miles) AS min_duration,
  MAX(trip_miles) AS max_duration,
  AVG(trip_miles) AS avg_duration
FROM
  trip_fact tp
  INNER JOIN transaction_fact tr on tp.transaction_id = tr.transaction_id;

-- COMMAND ----------

-- Count of NULL values in each column
SELECT
  SUM(
    CASE
      WHEN trip_start_timestamp IS NULL THEN 1
      ELSE 0
    END
  ) AS null_trip_start,
  SUM(
    CASE
      WHEN trip_end_timestamp IS NULL THEN 1
      ELSE 0
    END
  ) AS null_trip_end,
  SUM(
    CASE
      WHEN fare IS NULL THEN 1
      ELSE 0
    END
  ) AS null_fare,
  SUM(
    CASE
      WHEN trip_miles IS NULL THEN 1
      ELSE 0
    END
  ) AS null_distance
FROM
  trip_fact tp
  INNER JOIN transaction_fact tr on tp.transaction_id = tr.transaction_id;

-- COMMAND ----------

-- Group fare into bins for distribution analysis
SELECT
  CASE
    WHEN fare < 5 THEN '<\$5'
    WHEN fare BETWEEN 5
    AND 10 THEN '\$5-\$10'
    WHEN fare BETWEEN 10
    AND 20 THEN '\$10-\$20'
    WHEN fare BETWEEN 20
    AND 50 THEN '\$20-\$50'
    ELSE '>\$50'
  END AS fare_range,
  COUNT(*) AS trip_count
FROM
  transaction_fact
GROUP BY
  fare_range
ORDER BY
  fare_range;

-- COMMAND ----------

-- Group trip distances into bins for distribution analysis
SELECT
  CASE
    WHEN trip_miles < 1 THEN '<1 mile'
    WHEN trip_miles BETWEEN 1
    AND 5 THEN '1-5 miles'
    WHEN trip_miles BETWEEN 5
    AND 10 THEN '5-10 miles'
    ELSE '>10 miles'
  END AS distance_range,
  COUNT(*) AS trip_count
FROM
  trip_fact
GROUP BY
  distance_range
ORDER BY
  distance_range;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Initial Data analysis 

-- COMMAND ----------

-- Number of rides per hour of the day
SELECT
  HOUR(
    to_timestamp(trip_start_timestamp, 'MM/dd/yyyy hh:mm:ss a')
  ) AS trip_hour,
  COUNT(*) AS trip_count
FROM
  trip_fact
GROUP BY
  trip_hour
ORDER BY
  trip_hour;

-- COMMAND ----------

-- Number of rides per day of the week
SELECT
  DAYOFWEEK(
    to_date(
      (
        to_timestamp(
          trip_start_timestamp,
          'MM/dd/yyyy hh:mm:ss a'
        )
      )
    )
  ) AS day_of_week,
  COUNT(*) AS trip_count
FROM
  trip_fact
GROUP BY
  day_of_week
ORDER BY
  day_of_week;

-- COMMAND ----------

-- Top 10 most popular pickup locations
SELECT
  pickup_community_area,
  COUNT(*) AS trip_count
FROM
  trip_fact
GROUP BY
  pickup_community_area
ORDER BY
  trip_count DESC
LIMIT
  10;

-- COMMAND ----------

-- Top 10 most popular drop-off locations
SELECT
  dropoff_community_area,
  COUNT(*) AS trip_count
FROM
  trip_fact
GROUP BY
  dropoff_community_area
ORDER BY
  trip_count DESC
LIMIT
  10;

-- COMMAND ----------

-- Trips with very short duration but long distances (potential anomalies)
SELECT
  *
FROM
  trip_fact
WHERE
  trip_seconds < 60
  AND trip_miles > 10


-- COMMAND ----------

-- Trips with very long duration but short distances (potential anomalies)
SELECT
  *
FROM
  trip_fact
WHERE
  trip_seconds > 3600
  AND trip_miles < 1


-- COMMAND ----------

-- MAGIC %md
-- MAGIC The total number of outliers noticed are 1,686+8,468=10,154.
-- MAGIC That's less than 0.2% of the entire dataset.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Problem Statement
-- MAGIC Urban cities face challenges in managing taxi fleets efficiently and reducing traffic congestion, especially during peak hours. Taxi companies and regulatory agencies struggle to allocate vehicles optimally, leading to delays, higher operational costs, and customer dissatisfaction. Simultaneously, traffic congestion increases, causing delays and increasing emissions. Efficient fleet distribution is key to reducing congestion and ensuring that passengers receive timely services.
-- MAGIC
-- MAGIC By analyzing historical and real-time taxi trip data, taxi companies can optimize their fleet management by identifying high-demand areas and times, ensuring adequate taxi availability. City planners can also leverage this data to identify congestion hotspots and high-traffic routes. This data-driven approach allows for better fleet allocation, reduced wait times, and improved traffic flow, ultimately minimizing congestion and enhancing urban mobility.

-- COMMAND ----------

-- We need to identify the hours of the day when demand is highest. This will help allocate more taxis during those periods.
-- Identify peak demand hours
SELECT
  HOUR(
    to_timestamp(trip_start_timestamp, 'MM/dd/yyyy hh:mm:ss a')
  ) AS trip_hour,
  COUNT(*) AS trip_count
FROM
  trip_fact
GROUP BY
  trip_hour
ORDER BY
  trip_count DESC
LIMIT 
  10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Answer: The hours with the highest demands are 17, 16 and 15. That's between 3 and 5 PM. This does make sense, since that's when most office hours end.

-- COMMAND ----------

-- This query identifies which locations (e.g., pickup zones or neighborhoods) have the highest demand for taxis. Knowing high-demand areas helps ensure the right number of taxis are available.
-- Identify top 10 high-demand pickup locations
SELECT
  pickup_community_area,
  COUNT(*) AS total_pickups
FROM
  trip_fact
GROUP BY
  pickup_community_area
ORDER BY
  total_pickups DESC
LIMIT
  10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Answer: Areas 76 and 8 seem to have the most pickups.

-- COMMAND ----------

-- By looking at how trip durations vary throughout the day, taxi companies can determine if there are periods of inefficiency or under-utilization in their fleet.
-- Calculate average trip duration per hour to identify congestion periods
SELECT
  HOUR(to_timestamp(trip_start_timestamp, 'MM/dd/yyyy hh:mm:ss a')) AS hour_of_day,
  AVG(trip_seconds) AS avg_duration_minutes
FROM
  trip_fact
GROUP BY
  hour_of_day
ORDER BY
  avg_duration_minutes DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Answer: Fleet utilization dips during the the early hours of the day, and is used the most during 12 and 8 PM.
-- MAGIC

-- COMMAND ----------

-- Determine the most congested routes by analyzing trips with unusually long durations relative to distance traveled.
-- Identify congestion hotspots by finding trips with high duration-to-distance ratios
SELECT
  pickup_community_area,
  dropoff_community_area,
  AVG(trip_seconds / trip_miles) AS avg_duration_per_mile
FROM
  trip_fact
WHERE
  pickup_community_area != dropoff_community_area -- Exclude trips with the same pickup and dropoff area
GROUP BY
  pickup_community_area,
  dropoff_community_area
HAVING
  COUNT(*) > 1000 -- Filter for frequently traveled routes
ORDER BY
  avg_duration_per_mile DESC
LIMIT
  10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Answer: The most congested routes started from pickup area 24 and 3. (Area 0 is outside Chicago so it's not considered)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Recommendation for Traffic Congestion Reduction
-- MAGIC

-- COMMAND ----------

-- To minimize traffic congestion, the city could limit the number of taxis allowed in specific high-traffic areas during peak hours. You can identify these areas and times.
-- Identify locations and times with high traffic congestion during peak hours
SELECT
  pickup_community_area,
  HOUR(to_timestamp(trip_start_timestamp, 'MM/dd/yyyy hh:mm:ss a')) AS hour_of_day,
  COUNT(*) AS total_trips
FROM
  trip_fact
WHERE
  HOUR(to_timestamp(trip_start_timestamp, 'MM/dd/yyyy hh:mm:ss a')) IN (12,13,14,15,16,17,18,19,20) -- Peak hours obtained from analysis above
GROUP BY
  pickup_community_area,
  HOUR(to_timestamp(trip_start_timestamp, 'MM/dd/yyyy hh:mm:ss a'))
ORDER BY
  total_trips DESC
LIMIT
  10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### High-Demand Pickup Areas:
-- MAGIC - Community Area 76 appears most frequently in the top 10. This suggests that it is a very high-demand area for taxis during peak hours. The largest number of trips originate from this area, particularly between 6:00 PM and 8:00 PM (with the highest demand at 8:00 PM).
-- MAGIC - Community Area 8 is another high-demand area, especially during the mid-afternoon (3:00 PM to 6:00 PM).
-- MAGIC - Community Area 32 is also a high-traffic area, particularly between 4:00 PM and 5:00 PM.
-- MAGIC
-- MAGIC ###### Peak Demand Times:
-- MAGIC - The busiest hours are generally in the late afternoon to early evening, with 8:00 PM (hour 20) and 7:00 PM (hour 19) showing the highest number of trips overall.
-- MAGIC - Community Area 76 is particularly busy in the evening (6:00 PM to 8:00 PM).
-- MAGIC - Community Area 8 is busiest in the mid-to-late afternoon (3:00 PM to 6:00 PM).
-- MAGIC - Community Area 32 is relatively busy in the late afternoon (4:00 PM and 5:00 PM).
-- MAGIC
-- MAGIC ###### Potential Congestion Hotspots:
-- MAGIC - The large number of trips originating from Community Area 76 during the evening hours suggests that this area may experience significant congestion. Taxi companies and city planners could focus on regulating the number of taxis or traffic management in this area during the evening to mitigate traffic issues.
-- MAGIC - Community Areas 8 and 32 also show high taxi demand in the afternoon, making them potential hotspots for congestion during those hours.
-- MAGIC
-- MAGIC ###### Recommendations Based on the Results:
-- MAGIC
-- MAGIC For Taxi Fleet Management: Taxi companies should allocate more vehicles to Community Area 76 during the evening hours (particularly between 6:00 PM and 8:00 PM) to meet high demand. Similarly, more vehicles should be allocated to Community Areas 8 and 32 in the late afternoon.
-- MAGIC
-- MAGIC For Traffic Congestion Management: City planners could consider implementing measures to reduce traffic in Community Area 76 during peak evening hours (6:00 PM to 8:00 PM), as this area experiences significant taxi activity. Measures could include limiting taxi access by pooling to common routes, adjusting traffic light timings, or encouraging alternate routes.
-- MAGIC

-- COMMAND ----------

-- Determine the most congested routes from area 76
-- Identify congestion hotspots by finding trips with high duration-to-distance ratios
SELECT
  pickup_community_area,
  dropoff_community_area,
  AVG(trip_seconds / trip_miles) AS avg_duration_per_mile
FROM
  trip_fact
WHERE
  pickup_community_area != dropoff_community_area -- Exclude trips with the same pickup and dropoff area
GROUP BY
  pickup_community_area,
  dropoff_community_area
HAVING
  COUNT(*) > 1000 -- Filter for frequently traveled routes
ORDER BY
  avg_duration_per_mile DESC


-- COMMAND ----------

-- MAGIC %md
-- MAGIC If a filter is applied to the pickup community area table for the area 74. The most congested routes include from 74 to 4,10,25 and 24. Since they take moore than 5 minutes per mile.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Fleet Optimizations 

-- COMMAND ----------

WITH demand_by_hour AS (
  SELECT
    HOUR(to_timestamp(trip_start_timestamp, 'MM/dd/yyyy hh:mm:ss a')) AS hour_of_day,
    pickup_community_area,
    COUNT(*) AS total_trips
  FROM
    trip_fact
  GROUP BY
    HOUR(to_timestamp(trip_start_timestamp, 'MM/dd/yyyy hh:mm:ss a')),
    pickup_community_area
)
SELECT
  pickup_community_area,
  hour_of_day,
  total_trips,
  CASE
    WHEN total_trips > (
      SELECT
        AVG(total_trips)
      FROM
        demand_by_hour
    ) THEN 'Increase fleet'
    WHEN total_trips < 0.8 * (
      SELECT
        AVG(total_trips)
      FROM
        demand_by_hour
    ) THEN 'Decrease fleet'
    ELSE 'Maintain fleet'
  END AS fleet_recommendation
FROM
  demand_by_hour
ORDER BY
  total_trips DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 1. **High-Demand Pickup Areas**:
-- MAGIC    - **Community Area 76** consistently appears in the results, particularly in the late evening (6:00 PM to 8:00 PM). This suggests that there is significant demand for taxis in this area during those hours.
-- MAGIC    - **Community Area 8** also appears multiple times, indicating high demand in the afternoon to early evening (3:00 PM to 6:00 PM).
-- MAGIC    - **Community Area 32** appears in the list for the late afternoon (4:00 PM and 5:00 PM).
-- MAGIC
-- MAGIC 2. **Peak Demand Times**:
-- MAGIC    - **8:00 PM (hour 20)** and **7:00 PM (hour 19)** in **Community Area 76** have the highest number of trips, suggesting that this area sees very high demand for taxis in the evening.
-- MAGIC    - **Community Area 8** sees its peak demand earlier, between **3:00 PM and 6:00 PM**.
-- MAGIC    - **Community Area 32** sees demand in the late afternoon, around **4:00 PM and 5:00 PM**.
-- MAGIC
-- MAGIC 3. **Fleet Reallocation Recommendations**:
-- MAGIC    - The `CASE` statement in the query recommended an **"Increase fleet"** in all the top results, meaning that in these locations and times, the number of trips is significantly higher than the average, indicating a need for more taxis to meet demand.
-- MAGIC    - **No "Maintain fleet" recommendations** were made in this case, which suggests that the demand in these top 10 scenarios is high compared to the average across all hours and areas.
-- MAGIC
-- MAGIC ##### Actionable Insights:
-- MAGIC
-- MAGIC 1. **For Taxi Fleet Management**:
-- MAGIC    - Taxi companies should allocate additional taxis to **Community Area 76** in the evening hours (6:00 PM to 8:00 PM) to meet the high demand.
-- MAGIC    - Similarly, more taxis should be allocated to **Community Area 8** in the mid-afternoon to early evening (3:00 PM to 6:00 PM).
-- MAGIC    - **Community Area 32** should receive more taxis in the late afternoon (4:00 PM and 5:00 PM).
-- MAGIC    
-- MAGIC 2. **For Traffic Congestion Management**:
-- MAGIC    - Since these high-demand periods and locations are associated with heavy taxi activity, city planners might want to monitor traffic in **Community Area 76** during evening rush hours. Steps like implementing congestion pricing or adjusting traffic flow patterns could help alleviate traffic issues.
-- MAGIC    - **Community Area 8** may also require attention during the afternoon hours to mitigate potential congestion from increased taxi activity.
-- MAGIC
-- MAGIC ##### Conclusion:
-- MAGIC
-- MAGIC This query provides an effective data-driven approach for reallocating taxis during peak hours in high-demand areas. By increasing the taxi fleet in the identified areas and times, companies can improve service efficiency and reduce customer wait times.

-- COMMAND ----------



-- COMMAND ----------


