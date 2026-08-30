# Databricks notebook source
# Notebook: 00_test_pipeline_dummy_data
# Objetivo: probar el flujo medallion completo (bronze -> silver -> gold -> reporte)
# con datos dummy generados localmente, sin necesidad de una conexión real a Kafka.
#
# Usa por defecto rutas bajo dbfs:/tmp/... para no pisar los datos de bronze/silver/gold
# reales. Ajusta los widgets si quieres apuntar a otras rutas.

try:
    bronze_path = dbutils.widgets.get("bronze_path")
    silver_path = dbutils.widgets.get("silver_path")
    gold_path = dbutils.widgets.get("gold_path")
    report_path = dbutils.widgets.get("report_path")
    num_events = int(dbutils.widgets.get("num_events"))
except:
    bronze_path = "dbfs:/tmp/kafka_medallion_test/delta/bronze/events"
    silver_path = "dbfs:/tmp/kafka_medallion_test/delta/silver/events"
    gold_path = "dbfs:/tmp/kafka_medallion_test/delta/gold/aggregates"
    report_path = "dbfs:/tmp/kafka_medallion_test/reports/events_report.html"
    num_events = 100

import json
import random
import uuid
from datetime import datetime, timedelta

from pyspark.sql.functions import col, count, from_json, to_timestamp
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

# ---------------------------------------------------------------------------
# 1) Generar eventos dummy con la misma forma que produce 01_ingest_bronze
#    (columnas key, value, timestamp), donde `value` es el JSON del evento.
# ---------------------------------------------------------------------------
event_types = ["page_view", "click", "purchase", "signup", "error"]
users = [f"user_{i}" for i in range(1, 21)]
now = datetime.utcnow()

dummy_rows = []
for i in range(num_events):
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(users),
        "event_type": random.choice(event_types),
        "payload": json.dumps({"seq": i, "value": round(random.uniform(0, 100), 2)}),
        "ts": (now - timedelta(seconds=random.randint(0, 3600))).isoformat(),
    }
    dummy_rows.append((event["event_id"], json.dumps(event), now))

# Introducimos algunos eventos "sucios" (sin event_id) para validar el filtro de limpieza
for _ in range(max(1, num_events // 20)):
    dirty = {
        "event_id": None,
        "user_id": random.choice(users),
        "event_type": "unknown",
        "payload": "{}",
        "ts": now.isoformat(),
    }
    dummy_rows.append((None, json.dumps(dirty), now))

bronze_schema = StructType([
    StructField("key", StringType()),
    StructField("value", StringType()),
    StructField("timestamp", TimestampType()),
])

bronze_df = spark.createDataFrame(dummy_rows, schema=bronze_schema)

(bronze_df.write.format("delta").mode("overwrite").save(bronze_path))
print(f"Bronze: {bronze_df.count()} eventos escritos en {bronze_path}")

# ---------------------------------------------------------------------------
# 2) Transformar bronze -> silver (misma logica que 02_transform_silver,
#    pero en batch para simplificar la prueba con datos dummy).
# ---------------------------------------------------------------------------
payload_schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("payload", StringType()),
    StructField("ts", StringType()),
])

bronze_batch = spark.read.format("delta").load(bronze_path)

parsed = (
    bronze_batch
    .withColumn("json", from_json(col("value"), payload_schema))
    .select(
        col("key"),
        col("json.event_id").alias("event_id"),
        col("json.user_id").alias("user_id"),
        col("json.event_type").alias("event_type"),
        col("json.payload").alias("payload"),
        to_timestamp(col("json.ts")).alias("event_ts"),
        col("timestamp").alias("ingest_ts"),
    )
)

clean = parsed.filter(col("event_id").isNotNull())

(clean.write.format("delta").mode("overwrite").save(silver_path))
print(f"Silver: {clean.count()} eventos limpios escritos en {silver_path} "
      f"(descartados {parsed.count() - clean.count()} sin event_id)")

# ---------------------------------------------------------------------------
# 3) Agregar silver -> gold y generar reporte HTML (misma logica que
#    03_aggregate_gold_report).
# ---------------------------------------------------------------------------
silver = spark.read.format("delta").load(silver_path)

agg = (
    silver
    .groupBy("event_type")
    .agg(count("event_id").alias("count"))
    .orderBy(col("count").desc())
)

(agg.write.format("delta").mode("overwrite").save(gold_path))
print(f"Gold: agregados escritos en {gold_path}")

df = agg.toPandas()
html = df.to_html(index=False)

try:
    dbutils.fs.put(report_path, html, True)
    print(f"Reporte guardado en: {report_path}")
except Exception as e:
    print("No fue posible guardar el reporte con dbutils.fs.put:", e)

displayHTML(html)

# ---------------------------------------------------------------------------
# 4) Validaciones basicas del flujo
# ---------------------------------------------------------------------------
assert bronze_df.count() == num_events + max(1, num_events // 20), "Conteo inesperado en bronze"
assert clean.count() <= bronze_df.count(), "Silver no puede tener mas filas que bronze"
assert agg.count() > 0, "El agregado gold no debe estar vacio"
print("OK: flujo bronze -> silver -> gold -> reporte validado con datos dummy.")
