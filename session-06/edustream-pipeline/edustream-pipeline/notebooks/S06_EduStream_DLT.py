# Databricks notebook source
# MAGIC %md
# MAGIC # EduStream · Pipeline DLT
# MAGIC ## Delta Live Tables — ingestión streaming declarativa con expectativas
# MAGIC
# MAGIC > **Este notebook NO se ejecuta directamente.** Se configura como source code
# MAGIC > de un pipeline en Jobs & Pipelines → Create ETL Pipeline. DLT requiere su
# MAGIC > propio runtime; ejecutar las celdas a mano falla.
# MAGIC
# MAGIC ### Por qué DLT además del notebook batch
# MAGIC
# MAGIC El notebook principal (`S06_EduStream_Pipeline`) es batch e imperativo: define
# MAGIC paso a paso *cómo* procesar. DLT es **declarativo**: se define *qué* tabla debe
# MAGIC existir y con qué calidad, y el runtime resuelve el orden de ejecución, el
# MAGIC manejo de dependencias y la recuperación ante fallos.
# MAGIC
# MAGIC Este notebook demuestra el patrón streaming — el mismo pipeline medallion, pero
# MAGIC con tablas que se actualizan de forma incremental a medida que llegan archivos
# MAGIC nuevos al Volume.
# MAGIC
# MAGIC ### Configuración del pipeline
# MAGIC
# MAGIC | Campo | Valor |
# MAGIC |-------|-------|
# MAGIC | Nombre | `edustream_pipeline` |
# MAGIC | Source code | ruta a este notebook |
# MAGIC | Destination catalog | `workspace` |
# MAGIC | Destination schema | `edustream` |
# MAGIC | Compute | Serverless |

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA  = "edustream"
VOLUME  = f"/Volumes/{CATALOG}/{SCHEMA}/landing"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — ingestión streaming con Auto Loader
# MAGIC
# MAGIC `cloudFiles` (Auto Loader) detecta archivos nuevos de forma incremental y
# MAGIC mantiene un checkpoint. `schemaLocation` persiste el schema inferido para no
# MAGIC re-inferirlo en cada micro-batch. `inferColumnTypes` mejora los tipos respecto
# MAGIC al default de Auto Loader (que trataría todo como string).

# COMMAND ----------

@dlt.table(
    name="bronze_enrollments_dlt",
    comment="Ingestión streaming de inscripciones vía Auto Loader",
    table_properties={"quality": "bronze"},
)
def bronze_enrollments_dlt():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{VOLUME}/_schema_enrollments")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{VOLUME}/enrollments.csv")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — validación de calidad con expectativas
# MAGIC
# MAGIC Tres expectativas, cada una con una severidad distinta. Esto es el corazón del
# MAGIC entregable: elegir la severidad correcta para cada regla de negocio.
# MAGIC
# MAGIC | Expectativa | Tipo | Qué pasa si falla |
# MAGIC |-------------|------|-------------------|
# MAGIC | `pago_valido` | `expect_or_drop` | La fila se descarta (pago negativo = corrupto) |
# MAGIC | `course_id_presente` | `expect` | La fila se conserva, se cuenta como violación |
# MAGIC | `schema_intacto` | `expect_or_fail` | El pipeline se detiene (drift de schema) |

# COMMAND ----------

@dlt.table(
    name="silver_enrollments_dlt",
    comment="Inscripciones validadas y limpias",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("pago_valido", "payment_amount >= 0")
@dlt.expect("course_id_presente", "course_id IS NOT NULL")
@dlt.expect_or_fail("schema_intacto", "_rescued_data IS NULL")
def silver_enrollments_dlt():
    """
    pago_valido (drop): un pago negativo no tiene interpretación de negocio.
    course_id_presente (warn): puede ser estado transitorio; se conserva marcado.
    schema_intacto (fail): si Auto Loader rescató datos, la fuente cambió su
        schema — fail-fast antes de producir métricas sobre datos mal parseados.
    """
    return (
        dlt.read_stream("bronze_enrollments_dlt")
        .withColumn("payment_amount",
                    F.coalesce(F.col("payment_amount").cast("double"), F.lit(0.0)))
        .withColumn("is_paid", F.col("payment_amount") > 0)
        .withColumn("_processed_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — métrica de negocio
# MAGIC
# MAGIC Gold lee de silver en modo batch (`dlt.read`, no `dlt.read_stream`): una
# MAGIC agregación no tiene sentido como tabla streaming. DLT resuelve la dependencia
# MAGIC bronze → silver → gold automáticamente y la muestra en el DAG.

# COMMAND ----------

@dlt.table(
    name="gold_revenue_dlt",
    comment="Revenue e inscripciones agregados por curso",
    table_properties={"quality": "gold"},
)
def gold_revenue_dlt():
    return (
        dlt.read("silver_enrollments_dlt")
        .filter(F.col("status") != "refunded")
        .groupBy("course_id")
        .agg(
            F.count("*").alias("inscripciones"),
            F.countDistinct("user_id").alias("estudiantes_unicos"),
            F.round(F.sum("payment_amount"), 2).alias("revenue_total"),
            F.round(F.avg("payment_amount"), 2).alias("ticket_promedio"),
        )
        .withColumn("_computed_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación
# MAGIC
# MAGIC Tras `Start`:
# MAGIC - El **DAG** muestra 3 nodos: `bronze_enrollments_dlt` → `silver_enrollments_dlt`
# MAGIC   → `gold_revenue_dlt`.
# MAGIC - La pestaña **Data quality** muestra, por cada expectativa, cuántas filas
# MAGIC   pasaron y cuántas fallaron.
# MAGIC - Si `schema_intacto` falla, el run se marca como FAILED — eso es el
# MAGIC   `expect_or_fail` haciendo su trabajo.
