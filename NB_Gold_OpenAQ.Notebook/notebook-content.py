# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "aa1ee62d-2f24-4c14-b409-b89563d3e7fa",
# META       "default_lakehouse_name": "M_Lakehouse_Itransition",
# META       "default_lakehouse_workspace_id": "95ebcbcf-35b4-4eda-bae3-344163c79c6c",
# META       "known_lakehouses": [
# META         {
# META           "id": "aa1ee62d-2f24-4c14-b409-b89563d3e7fa"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df_openaq_silver = spark.read.table("Silver_OpenAQ")

display(df_openaq_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_openaq_silver.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import avg, min, max, count

df_gold_openaq = (
    df_openaq_silver
    .groupBy("pollutant", "units")
    .agg(
        avg("measurement_value").alias("avg_value"),
        min("measurement_value").alias("min_value"),
        max("measurement_value").alias("max_value"),
        count("*").alias("measurement_count")
    )
    .orderBy("pollutant")
)

display(df_gold_openaq)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_openaq.write.mode("overwrite").saveAsTable("Gold_OpenAQ")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# OpenAQ finished here and NYC taxi started


# CELL ********************

df_taxi_silver = spark.read.table("Silver_NYC_Taxi")

df_taxi_silver.printSchema()
display(df_taxi_silver.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import to_date, col, avg, count

df_taxi_silver = spark.read.table("Silver_NYC_Taxi")

df_gold_taxi_daily = (
    df_taxi_silver
    .withColumn("trip_date", to_date(col("tpep_pickup_datetime")))
    .groupBy("trip_date")
    .agg(
        count("*").alias("trip_count"),
        avg("trip_distance").alias("avg_trip_distance"),
        avg("total_amount").alias("avg_total_amount"),
        avg("tip_amount").alias("avg_tip_amount"),
        avg("passenger_count").alias("avg_passenger_count")
    )
    .orderBy("trip_date")
)

display(df_gold_taxi_daily)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DROP TABLE IF EXISTS Gold_NYC_Taxi_Daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_taxi_daily.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("Gold_NYC_Taxi_Daily")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("Gold_NYC_Taxi_Daily").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
