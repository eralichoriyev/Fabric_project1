# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e9f99a26-f6b0-40b8-8267-9bfb289e66ef",
# META       "default_lakehouse_name": "UnifiedLakehouse",
# META       "default_lakehouse_workspace_id": "2cb788df-1a0e-4a24-9bba-a19496952689",
# META       "known_lakehouses": [
# META         {
# META           "id": "e9f99a26-f6b0-40b8-8267-9bfb289e66ef"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

taxi_bronze_path = "Files/bronze/nyc_taxi/yellow_tripdata_2025-01.parquet"

df_taxi_bronze = spark.read.parquet(taxi_bronze_path)

df_taxi_bronze.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_bronze.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_bronze.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "PULocationID",
    "DOLocationID"
).show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df_taxi_silver = (
    df_taxi_bronze
    .filter(col("tpep_pickup_datetime").isNotNull())
    .filter(col("tpep_dropoff_datetime").isNotNull())
    .filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
    .filter(col("trip_distance") > 0)
)

df_taxi_silver.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_taxi_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_nyc_taxi")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) AS row_count
# MAGIC FROM silver_nyc_taxi;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

openaq_bronze_path = "Files/bronze/openaq/openaq_latest_8118.json"

df_openaq_bronze = spark.read.json(openaq_bronze_path)

df_openaq_bronze.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import explode, col

df_openaq_silver_flat = (
    df_openaq_bronze
    .select(explode(col("results")).alias("r"))
)

df_openaq_silver_flat.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, to_timestamp

df_openaq_silver = (
    df_openaq_silver_flat
    .select(
        to_timestamp(col("r.datetime.utc")).alias("measurement_time_utc"),
        col("r.value").alias("value"),
        col("r.coordinates.latitude").alias("latitude"),
        col("r.coordinates.longitude").alias("longitude"),
        col("r.locationsId").alias("location_id"),
        col("r.sensorsId").alias("sensor_id")
    )
)

df_openaq_silver.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_openaq_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_openaq_air_quality")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) FROM silver_openaq_air_quality

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_openaq_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_openaq_air_quality_latest")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM silver_openaq_air_quality_latest;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gdp_bronze_path = "Files/bronze/economics/worldbank_gdp_usa.json"

df_gdp_bronze = spark.read.json(gdp_bronze_path)

df_gdp_bronze.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gdp_bronze.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils

mssparkutils.fs.ls("Files/bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils

mssparkutils.fs.ls("Files/bronze/economics")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gdp_path = "Files/bronze/economics/worldbank_gdp_usa.json"

df_gdp_raw = (
    spark.read
    .option("multiLine", "true")
    .json(gdp_path)
)

df_gdp_raw.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gdp_path = "Files/bronze/economics/worldbank_gdp_usa.json"

df_gdp_text = spark.read.text(gdp_path)
df_gdp_text.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

parsed = json.loads(raw_json)

# element [1] contains the records
gdp_records = parsed[1]

# FORCE value to float to avoid Long vs Double conflicts
for r in gdp_records:
    if r["value"] is not None:
        r["value"] = float(r["value"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)

gdp_schema = StructType([
    StructField("indicator", StructType([
        StructField("id", StringType(), True),
        StructField("value", StringType(), True)
    ]), True),
    StructField("country", StructType([
        StructField("id", StringType(), True),
        StructField("value", StringType(), True)
    ]), True),
    StructField("countryiso3code", StringType(), True),
    StructField("date", StringType(), True),
    StructField("value", DoubleType(), True),   # ← now consistent
    StructField("unit", StringType(), True),
    StructField("obs_status", StringType(), True),
    StructField("decimal", LongType(), True)
])

df_gdp_bronze = spark.createDataFrame(gdp_records, schema=gdp_schema)

df_gdp_bronze.printSchema()
df_gdp_bronze.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gdp_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_worldbank_gdp_usa")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT year(date) AS year, value
# MAGIC FROM silver_worldbank_gdp_usa
# MAGIC ORDER BY year DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM silver_worldbank_gdp_usa
# MAGIC ORDER BY date DESC
# MAGIC LIMIT 5;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gdp_gold = (
    spark.table("silver_worldbank_gdp_usa")
    .select(
        col("indicator.id").alias("indicator_id"),
        col("indicator.value").alias("indicator_name"),
        col("country.id").alias("country_id"),
        col("country.value").alias("country_name"),
        col("countryiso3code"),
        col("date").cast("int").alias("year"),
        col("value").alias("gdp_usd")
    )
)

df_gdp_gold.printSchema()
df_gdp_gold.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_gdp_gold
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold_worldbank_gdp_usa")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM gold_worldbank_gdp_usa
# MAGIC ORDER BY year DESC
# MAGIC LIMIT 5;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SHOW TABLES;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) AS rows_gdp
# MAGIC FROM gold_worldbank_gdp_usa;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE TABLE gold_worldbank_gdp_usa;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
