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

df_silver_taxi = spark.read.table("Silver_NYC_Taxi")

display(df_silver_taxi)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, to_date, count, sum

df_gold_daily = (
    df_silver_taxi
    .withColumn("trip_date", to_date(col("tpep_pickup_datetime")))
    .groupBy("trip_date")
    .agg(
        count("*").alias("total_trips"),
        sum("passenger_count").alias("total_passengers"),
        sum("trip_distance").alias("total_distance")
    )
    .orderBy("trip_date")
)

display(df_gold_daily)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_daily.write.mode("overwrite").saveAsTable("Gold_NYC_Taxi_Daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SELECT * FROM Gold_NYC_Taxi_Daily ORDER BY trip_date").show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# here silver transformation ends 
