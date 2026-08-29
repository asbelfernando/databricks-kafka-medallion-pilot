# Databricks notebook source
# Notebook: 03_aggregate_gold_report
# Objetivo: crear agregados (gold) y generar un reporte HTML

try:
    silver_path = dbutils.widgets.get("silver_path")
    gold_path = dbutils.widgets.get("gold_path")
    report_path = dbutils.widgets.get("report_path")
except:
    silver_path = "dbfs:/delta/silver/events"
    gold_path = "dbfs:/delta/gold/aggregates"
    report_path = "dbfs:/reports/events_report.html"

from pyspark.sql.functions import window, col, count

# Leer batch desde silver (para reportes periódicos usamos batch)
silver = spark.read.format("delta").load(silver_path)

# Ejemplo de agregación: conteo por tipo de evento en ventana de 1 hora
agg = (
    silver
    .groupBy("event_type")
    .agg(count("event_id").alias("count"))
    .orderBy(col("count").desc())
)

# Escribir gold
agg.write.format("delta").mode("overwrite").save(gold_path)

# Generar reporte HTML (solo para volúmenes pequeños — en pilot)
df = agg.toPandas()
html = df.to_html(index=False)

# Guardar HTML en DBFS
try:
    dbutils.fs.put(report_path, html, True)
    print(f"Reporte guardado en: {report_path}")
except Exception as e:
    print("No fue posible guardar el reporte con dbutils.fs.put:", e)

# Mostrar en notebook
displayHTML(html)
