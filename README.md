# Taximizer - Optimizing Taxi Fleet Management and Reducing Traffic Congestion

## Summary
The goal of this project is to optimize taxi fleet management and reduce traffic congestion in urban settings by leveraging Big Data Management tools like Apache Spark and Databricks. Urban taxi companies often face the challenge of efficiently allocating their fleets to meet fluctuating demand, which, if mismanaged, can lead to customer dissatisfaction, increased operational costs, and congestion in high-traffic areas. By analyzing historical and real-time taxi trip data, this project aims to identify patterns in taxi demand, uncover peak usage times, and reveal congestion hotspots. The insights gained from this analysis are intended to inform fleet allocation strategies, enhance service efficiency, and support city planners in managing traffic flow.
To achieve this, we utilized a data pipeline that integrates both historical and real-time data through API calls. This data was cleansed, transformed, and stored in cloud-based Parquet files, which allowed for scalable and efficient data processing. Using Apache Spark’s distributed processing capabilities, we created a Data Warehouse with structured tables, enabling in-depth analysis of taxi trip patterns, including trip duration, fare distributions, and peak travel times (Spark, n.d.). The data analysis focused on identifying high-demand pickup and drop-off areas, which are critical for fleet resource optimization.

### Dataset used
 [https://data.cityofchicago.org/Transportation/Taxi-Trips-2024-/ajtu-isnz/](url)
