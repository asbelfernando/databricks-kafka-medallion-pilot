# Databricks notebook source
# Notebook: 02_transform_silver
# Objetivo: leer bronze (Delta), parsear payloads, limpiar y escribir silver

try:
    bronze_path = dbutils.widgets.get("bronze_path")
    silver_path = dbutils.widgets.get("silver_path")
    checkpoint = dbutils.widgets.get("checkpoint")
except:
    bronze_path = "dbfs:/delta/bronze/events"
    silver_path = "dbfs:/delta/silver/events"
    checkpoint = "dbfs:/delta/_checkpoints/silver"

from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# Definir schema del payload (ajustar según tu evento)
payload_schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("payload", StringType()),
    StructField("ts", StringType())
])

# Leer stream desde Delta (bronze)
bronze = (
    spark.readStream
         .format("delta")
         .load(bronze_path)
)

# Parsear JSON en `value`
parsed = (
    bronze
    .withColumn("json", from_json(col("value"), payload_schema))
    .select(
        col("key"),
        col("json.event_id").alias("event_id"),
        col("json.user_id").alias("user_id"),
        col("json.event_type").alias("event_type"),
        col("json.payload").alias("payload"),
        to_timestamp(col("json.ts")).alias("event_ts"),
        col("timestamp").alias("ingest_ts")
    )
)

# Limpiezas básicas: filtrar nulos
clean = parsed.filter(col("event_id").isNotNull())

# Escribir a Delta (silver)
(write_stream := clean.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .start(silver_path)
)

display(write_stream)
