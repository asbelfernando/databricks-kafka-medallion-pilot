# Databricks Kafka Medallion Pilot

Proyecto piloto: notebooks para conectarse a un topic de Kafka desde Databricks (Spark Structured Streaming), implementar una arquitectura medallion (bronze/silver/gold) y generar un reporte en línea.

## Objetivo
- Ingestar eventos desde Kafka hacia Delta (Bronze).
- Transformar y limpiar datos (Silver).
- Agregar y producir artefactos de consumo/visualización (Gold / reporte HTML).

## Pre-requisitos
- Cuenta y workspace en Databricks.
- Cluster Databricks runtime 12.x/13.x o superior (Spark 3.x) con soporte Delta Lake.
- Acceso a un cluster con librerías para Kafka (normalmente incluido en Databricks runtime).
- Un topic de Kafka y credenciales (bootstrap servers, opciones de seguridad si aplica).
- Un storage montado (DBFS, ADLS, S3) para persistencia Delta y checkpoints.

## Estructura del repositorio
- `notebooks/01_ingest_bronze.py` — Ingesta desde Kafka a Delta (bronze).
- `notebooks/02_transform_silver.py` — Transformaciones y limpieza (silver).
- `notebooks/03_aggregate_gold_report.py` — Agregaciones, escritura gold y generación de reporte HTML.
- `config.sample.json` — Plantilla de configuración (reemplazar valores).
- `.gitignore`, `LICENSE`

## Cómo importar a Databricks
1. En Databricks Workspace > Import > File, seleccionar los archivos de `notebooks/*.py`.
2. Crear un cluster con runtime recomendado y montar el storage si es necesario.
3. Crear widgets en cada notebook para pasar `kafka_servers`, `topic`, `bronze_path`, etc. (ver notebooks).

## Ejecución básica
- Ejecutar `01_ingest_bronze` en modo streaming (Start). Reemplace los placeholders de conexión.
- Ejecutar `02_transform_silver` como job o en streaming continuo para normalizar datos.
- Ejecutar `03_aggregate_gold_report` periódicamente para generar reportes HTML bajo `dbfs:/reports/`.

## Cómo subir el proyecto a GitHub
```bash
cd databricks-kafka-medallion-pilot
git init
git add .
git commit -m "Initial medallion pilot notebooks"
# Añade tu remoto (ejemplo):
git remote add origin https://github.com/asbelfernando/databricks-kafka-medallion-pilot.git
git push -u origin main
```

## Notas
- Los notebooks contienen placeholders (variables) que debes ajustar a tu entorno.
- Para producción, usar secretos (Databricks Secrets) y no poner credenciales en texto plano.

---
Feedback y mejoras bienvenidas.