# Databricks notebook source
# MAGIC %md
# MAGIC # Airline Operational Performance Analysis (2015)
# MAGIC
# MAGIC ## Capstone Project
# MAGIC Operational Bottleneck Identification and Performance Optimization in the U.S. Airline Industry
# MAGIC
# MAGIC ### Objective
# MAGIC This notebook performs descriptive analytics to identify:
# MAGIC - Airline-level delay and cancellation patterns
# MAGIC - Airport congestion effects
# MAGIC - Seasonal and hourly delay trends
# MAGIC - Route-level operational bottlenecks

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG tables;
# MAGIC USE SCHEMA default;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executive Snapshot
# MAGIC
# MAGIC This section provides an overall industry-level performance summary.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_flights,
# MAGIC     ROUND(SUM(IS_DELAYED_ARRIVAL)/COUNT(*),4) AS overall_arrival_delay_rate,
# MAGIC     ROUND(SUM(CANCELLED)/COUNT(*),4) AS overall_cancel_rate,
# MAGIC     ROUND(AVG(ARRIVAL_DELAY),2) AS avg_arrival_delay_minutes
# MAGIC FROM finally_cleaned_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interpretation
# MAGIC
# MAGIC - This provides a benchmark for airline performance comparison.
# MAGIC - Overall delay rate establishes industry-wide operational baseline.
# MAGIC - Cancellation rate indicates systemic reliability level.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Airline-Level Performance Analysis
# MAGIC
# MAGIC This section evaluates operational efficiency across airlines.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     AIRLINE,
# MAGIC     COUNT(*) AS total_flights,
# MAGIC     ROUND(SUM(IS_DELAYED_ARRIVAL)/COUNT(*),4) AS arrival_delay_rate,
# MAGIC     ROUND(SUM(IS_DELAYED_DEPARTURE)/COUNT(*),4) AS departure_delay_rate,
# MAGIC     ROUND(SUM(CANCELLED)/COUNT(*),4) AS cancel_rate,
# MAGIC     ROUND(AVG(ARRIVAL_DELAY),2) AS avg_arrival_delay
# MAGIC FROM finally_cleaned_data
# MAGIC GROUP BY AIRLINE
# MAGIC ORDER BY arrival_delay_rate DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Observations
# MAGIC
# MAGIC - Airlines show significant variation in delay rates.
# MAGIC - Some carriers demonstrate consistently higher cancellation rates.
# MAGIC - Differences suggest operational strategy or hub exposure impact.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Airport Congestion Analysis
# MAGIC
# MAGIC This section identifies airports contributing to higher delays.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     ORIGIN_AIRPORT,
# MAGIC     COUNT(*) AS total_flights,
# MAGIC     ROUND(SUM(IS_DELAYED_ARRIVAL)/COUNT(*),4) AS delay_rate,
# MAGIC     ROUND(SUM(CANCELLED)/COUNT(*),4) AS cancel_rate,
# MAGIC     ROUND(AVG(ORIGIN_CONGESTION),2) AS avg_congestion
# MAGIC FROM finally_cleaned_data
# MAGIC GROUP BY ORIGIN_AIRPORT
# MAGIC HAVING COUNT(*) > 500
# MAGIC ORDER BY delay_rate DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Observations
# MAGIC
# MAGIC - Major hub airports exhibit higher congestion metrics.
# MAGIC - Elevated congestion correlates with increased delay rates.
# MAGIC - Infrastructure and traffic density likely influence performance.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Monthly Delay and Cancellation Trends
# MAGIC
# MAGIC This section evaluates seasonality in operational performance.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     MONTH,
# MAGIC     ROUND(SUM(IS_DELAYED_ARRIVAL)/COUNT(*),4) AS delay_rate,
# MAGIC     ROUND(SUM(CANCELLED)/COUNT(*),4) AS cancel_rate
# MAGIC FROM finally_cleaned_data
# MAGIC GROUP BY MONTH
# MAGIC ORDER BY MONTH;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Observations
# MAGIC
# MAGIC - Seasonal variation is observed in delay frequency.
# MAGIC - Winter months may show higher cancellation rates.
# MAGIC - Travel demand peaks influence operational pressure.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC     DEPARTURE_HOUR,
# MAGIC     ROUND(SUM(IS_DELAYED_ARRIVAL)/COUNT(*),4) AS delay_rate
# MAGIC FROM finally_cleaned_data
# MAGIC GROUP BY DEPARTURE_HOUR
# MAGIC ORDER BY DEPARTURE_HOUR;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Observations
# MAGIC
# MAGIC - Delay rates increase progressively across the day.
# MAGIC - Peak disruption observed in late afternoon and evening.
# MAGIC - Accumulated delays likely propagate into later departures.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Route-Level Bottleneck Analysis
# MAGIC
# MAGIC This section identifies routes with persistent delay risk.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     ROUTE,
# MAGIC     COUNT(*) AS total_flights,
# MAGIC     ROUND(SUM(IS_DELAYED_ARRIVAL)/COUNT(*),4) AS delay_rate,
# MAGIC     ROUND(SUM(CANCELLED)/COUNT(*),4) AS cancel_rate
# MAGIC FROM finally_cleaned_data
# MAGIC GROUP BY ROUTE
# MAGIC HAVING COUNT(*) > 300
# MAGIC ORDER BY delay_rate DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC # Overall Descriptive Insights
# MAGIC
# MAGIC - Airline-level performance varies significantly across carriers.
# MAGIC - Hub congestion contributes to delay propagation.
# MAGIC - Seasonal and hourly trends reveal structural operational pressure.
# MAGIC - Certain routes consistently demonstrate higher delay risk.
# MAGIC
# MAGIC These descriptive findings establish the foundation for statistical validation and predictive modeling.