# Databricks notebook source
# Notebook: 01_ingest_bronze
# Objetivo: consumir eventos desde Kafka y escribirlos a Delta (bronze)

# Config: los valores pueden provenir de widgets de Databricks o del archivo config
try:
    kafka_bootstrap = dbutils.widgets.get("kafka_bootstrap_servers")
    topic = dbutils.widgets.get("kafka_topic")
    bronze_path = dbutils.widgets.get("bronze_path")
    checkpoint = dbutils.widgets.get("checkpoint")
except:
    kafka_bootstrap = "<BOOTSTRAP_SERVERS>"
    topic = "<TOPIC>"
    bronze_path = "dbfs:/delta/bronze/events"
    checkpoint = "dbfs:/delta/_checkpoints/bronze"

from pyspark.sql.functions import col

# Lectura streaming desde Kafka
kafka_df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", kafka_bootstrap)
         .option("subscribe", topic)
         .option("startingOffsets", "latest")
         .load()
)

# Mantener key, value, timestamp
events = kafka_df.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value", "timestamp")

# Escritura a Delta (bronze)
write_stream = (
    events.writeStream
          .format("delta")
          .outputMode("append")
          .option("checkpointLocation", checkpoint)
          .start(bronze_path)
)

display(write_stream)
