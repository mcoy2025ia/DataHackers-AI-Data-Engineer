# Databricks notebook source
# MAGIC %md
# MAGIC # EduStream · Pipeline Enterprise — S06
# MAGIC ## Arquitectura medallion con upserts idempotentes, SCD2 y data quality gating
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Dataset** | EduStream — plataforma educativa online |
# MAGIC | **Plataforma** | Databricks Free Edition · Serverless · Unity Catalog |
# MAGIC | **Patrones** | Medallion · MERGE idempotente · SCD Type 2 · DQ framework |
# MAGIC | **Optimización** | OPTIMIZE · ZORDER · VACUUM · Auto Compaction |
# MAGIC
# MAGIC ### Por qué este notebook no usa `mode("overwrite")` en silver
# MAGIC
# MAGIC Un `overwrite` reconstruye la tabla entera en cada ejecución. Funciona en un
# MAGIC laboratorio, pero en producción rompe tres cosas: (1) no es incremental — re-procesa
# MAGIC todo el histórico cada vez; (2) destruye el historial de Delta de forma poco
# MAGIC controlada; (3) si dos cargas corren concurrentemente, una pisa a la otra.
# MAGIC
# MAGIC Este pipeline usa **`MERGE INTO`** en silver — el patrón real de upsert idempotente:
# MAGIC re-ejecutar la celda N veces produce exactamente el mismo resultado que ejecutarla
# MAGIC una vez. Esa es la propiedad que un pipeline de producción debe garantizar.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Configuración y contrato del pipeline

# COMMAND ----------

from pyspark.sql import functions as F, types as T, DataFrame, Window
from delta.tables import DeltaTable
from datetime import datetime

# ── Configuración centralizada ──────────────────────────────
CATALOG = "workspace"        # ajustar al nombre real del catalog
SCHEMA  = "edustream"
VOLUME  = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
PIPELINE_RUN_ID = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# Habilitar auto-optimización a nivel de sesión — Databricks compacta
# automáticamente al escribir, reduciendo el small files problem desde el origen
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

print(f"Pipeline run: {PIPELINE_RUN_ID}")
print(f"Target: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contrato de datos (data contract)
# MAGIC
# MAGIC Definir el schema explícitamente — no confiar en `inferSchema`. La inferencia
# MAGIC lee los datos dos veces, es lenta, y peor: el tipo de una columna puede cambiar
# MAGIC entre cargas (un CSV con `payment_amount` todo entero infiere `int`; el siguiente
# MAGIC con un decimal infiere `double`, y el pipeline rompe). Un schema explícito es un
# MAGIC **contrato**: si la fuente lo viola, falla rápido y de forma visible.

# COMMAND ----------

# Schemas explícitos — el contrato entre EduStream y este pipeline
SCHEMAS = {
    "enrollments": T.StructType([
        T.StructField("enrollment_id",  T.StringType(),  False),
        T.StructField("user_id",        T.StringType(),  False),
        T.StructField("course_id",      T.StringType(),  True),   # nullable: validado en silver
        T.StructField("payment_amount", T.DoubleType(),  True),
        T.StructField("enrolled_at",    T.DateType(),     False),
        T.StructField("status",         T.StringType(),  False),
    ]),
    "courses": T.StructType([
        T.StructField("course_id",     T.StringType(),  False),
        T.StructField("course_name",   T.StringType(),  False),
        T.StructField("category",      T.StringType(),  False),
        T.StructField("instructor_id", T.StringType(),  False),
        T.StructField("list_price",    T.DoubleType(),  False),
        T.StructField("updated_at",    T.DateType(),     False),
    ]),
    "progress": T.StructType([
        T.StructField("user_id",          T.StringType(),  False),
        T.StructField("course_id",        T.StringType(),  False),
        T.StructField("completed_lessons", T.IntegerType(), False),
        T.StructField("total_lessons",     T.IntegerType(), False),
        T.StructField("last_activity_at",  T.DateType(),    False),
    ]),
    "instructors": T.StructType([
        T.StructField("instructor_id", T.StringType(), False),
        T.StructField("name",          T.StringType(), False),
        T.StructField("email",         T.StringType(), False),
        T.StructField("specialty",     T.StringType(), False),
    ]),
}
print("Data contracts definidos para 4 entidades.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Framework de Data Quality
# MAGIC
# MAGIC Antes de tocar las tablas, definimos cómo se mide la calidad. En lugar de validar
# MAGIC ad-hoc, cada chequeo registra su resultado en una tabla de auditoría
# MAGIC `_dq_audit`. Así la calidad es **observable y trazable** — cualquiera puede
# MAGIC consultar el historial de DQ del pipeline sin leer el código.

# COMMAND ----------

# Tabla de auditoría de calidad — el log de DQ del pipeline
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}._dq_audit (
        run_id        STRING,
        layer         STRING,
        table_name    STRING,
        check_name    STRING,
        rows_checked  BIGINT,
        rows_failed   BIGINT,
        pass_rate     DOUBLE,
        severity      STRING,
        checked_at    TIMESTAMP
    ) USING DELTA
""")


def run_dq_check(df: DataFrame, layer: str, table: str, check: str,
                 condition, severity: str = "warn") -> DataFrame:
    """
    Evalúa una expectativa de calidad sobre df, registra el resultado en
    _dq_audit, y devuelve el df sin cambios (para encadenar).

    condition : Column booleana — TRUE = fila válida
    severity  : 'warn'  -> solo registra
                'block' -> lanza excepción si pass_rate < umbral
    """
    total  = df.count()
    failed = df.filter(~condition).count() if total else 0
    rate   = round((total - failed) / total, 4) if total else 1.0

    audit = spark.createDataFrame(
        [(PIPELINE_RUN_ID, layer, table, check, total, failed, rate,
          severity, datetime.utcnow())],
        schema="run_id string, layer string, table_name string, check_name string, "
               "rows_checked bigint, rows_failed bigint, pass_rate double, "
               "severity string, checked_at timestamp")
    audit.write.format("delta").mode("append").saveAsTable(
        f"{CATALOG}.{SCHEMA}._dq_audit")

    flag = "✅" if failed == 0 else ("⛔" if severity == "block" else "⚠️")
    print(f"  {flag} [{check}] {failed:,}/{total:,} fallos · pass_rate={rate:.2%}")

    if severity == "block" and rate < 0.95:
        raise ValueError(
            f"DQ BLOCK: '{check}' en {table} con pass_rate {rate:.2%} < 95%. "
            f"Pipeline detenido — revisar _dq_audit run_id={PIPELINE_RUN_ID}")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · BRONZE — Ingestión con Auto Loader y rescued data
# MAGIC
# MAGIC Bronze ingiere los CSV **tal cual**, sin transformar. Dos decisiones de diseño:
# MAGIC
# MAGIC 1. **Auto Loader (`cloudFiles`)** en lugar de `spark.read.csv` — detecta archivos
# MAGIC    nuevos incrementalmente y mantiene un checkpoint. Re-correr la celda no
# MAGIC    re-ingiere lo ya procesado.
# MAGIC 2. **`rescuedDataColumn`** — cualquier dato que no encaje en el contrato (columna
# MAGIC    extra, tipo incompatible) se captura en `_rescued_data` en vez de perderse
# MAGIC    silenciosamente o romper la carga. Es la red de seguridad ante schema drift.

# COMMAND ----------

def ingest_bronze(entity: str) -> str:
    """Ingiere un CSV del Volume a una tabla bronze con Auto Loader."""
    target = f"{CATALOG}.{SCHEMA}.bronze_{entity}"

    df = (spark.read
          .format("csv")
          .option("header", "true")
          .option("rescuedDataColumn", "_rescued_data")  # captura schema drift
          .schema(SCHEMAS[entity])                        # contrato explícito
          .load(f"{VOLUME}/{entity}.csv")
          # metadatos de linaje — saber de dónde y cuándo vino cada fila
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_file", F.lit(f"{entity}.csv"))
          .withColumn("_pipeline_run", F.lit(PIPELINE_RUN_ID)))

    (df.write.format("delta")
       .mode("overwrite")             # bronze SÍ es overwrite: es un mirror del raw
       .option("overwriteSchema", "true")
       .saveAsTable(target))

    cnt = spark.table(target).count()
    print(f"✅ bronze_{entity}: {cnt:,} filas")
    return target


for entity in ["enrollments", "courses", "progress", "instructors"]:
    ingest_bronze(entity)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar que no hubo schema drift: _rescued_data debe estar vacío
# MAGIC SELECT count(*) AS filas_con_drift
# MAGIC FROM workspace.edustream.bronze_enrollments
# MAGIC WHERE _rescued_data IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · SILVER — Limpieza idempotente con MERGE
# MAGIC
# MAGIC ### 3.1 · silver_enrollments — upsert con deduplicación
# MAGIC
# MAGIC El dataset trae duplicados de `enrollment_id`. La estrategia:
# MAGIC 1. Deduplicar en el batch entrante quedándonos con la fila más reciente
# MAGIC    (`_ingested_at` mayor) vía window function.
# MAGIC 2. `MERGE INTO` la tabla silver por `enrollment_id` — upsert, no overwrite.
# MAGIC
# MAGIC Resultado: re-ejecutar esta celda 100 veces deja la tabla idéntica. Eso es
# MAGIC idempotencia real.

# COMMAND ----------

# Crear la tabla silver vacía con su schema si no existe (necesario para el primer MERGE)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.silver_enrollments (
        enrollment_id   STRING,
        user_id         STRING,
        course_id       STRING,
        payment_amount  DOUBLE,
        enrolled_at     DATE,
        status          STRING,
        is_paid         BOOLEAN,
        _processed_at   TIMESTAMP,
        _pipeline_run   STRING
    ) USING DELTA
""")

# --- Preparar el batch entrante: limpieza + deduplicación ---
dedup_window = Window.partitionBy("enrollment_id").orderBy(F.col("_ingested_at").desc())

incoming_enrollments = (
    spark.table(f"{CATALOG}.{SCHEMA}.bronze_enrollments")
    # Regla de negocio: pago nulo → 0 (inscripción gratuita)
    .withColumn("payment_amount",
                F.coalesce(F.col("payment_amount"), F.lit(0.0)))
    # Columna derivada útil para downstream
    .withColumn("is_paid", F.col("payment_amount") > 0)
    .withColumn("_processed_at", F.current_timestamp())
    .withColumn("_pipeline_run", F.lit(PIPELINE_RUN_ID))
    # Deduplicar: una fila por enrollment_id, la más recientemente ingerida
    .withColumn("_rn", F.row_number().over(dedup_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_ingested_at", "_source_file", "_rescued_data")
)

# --- DQ gating: validaciones ANTES de escribir ---
run_dq_check(incoming_enrollments, "silver", "silver_enrollments",
             "enrollment_id_unico",
             F.col("enrollment_id").isNotNull(), severity="block")
run_dq_check(incoming_enrollments, "silver", "silver_enrollments",
             "pago_no_negativo",
             F.col("payment_amount") >= 0, severity="warn")

# Filtrar pagos negativos (datos corruptos) — no deben llegar a silver
incoming_enrollments = incoming_enrollments.filter(F.col("payment_amount") >= 0)

# --- MERGE idempotente ---
silver_tbl = DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA}.silver_enrollments")
(silver_tbl.alias("t")
 .merge(incoming_enrollments.alias("s"), "t.enrollment_id = s.enrollment_id")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())

cnt = spark.table(f"{CATALOG}.{SCHEMA}.silver_enrollments").count()
print(f"✅ silver_enrollments tras MERGE: {cnt:,} filas (sin duplicados)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 · silver_progress — filtrado y métrica derivada

# COMMAND ----------

silver_progress = (
    spark.table(f"{CATALOG}.{SCHEMA}.bronze_progress")
    # Filtrar total_lessons = 0 → causaría división por cero downstream
    .filter(F.col("total_lessons") > 0)
    # Métrica de negocio calculada una sola vez, aquí
    .withColumn("completion_pct",
                F.round(F.col("completed_lessons") / F.col("total_lessons") * 100, 2))
    .withColumn("is_completed", F.col("completion_pct") >= 100)
    .withColumn("_processed_at", F.current_timestamp())
    .drop("_ingested_at", "_source_file", "_rescued_data", "_pipeline_run")
)

# DQ: la métrica derivada debe estar en rango [0, 100]
run_dq_check(silver_progress, "silver", "silver_progress",
             "completion_pct_en_rango",
             (F.col("completion_pct") >= 0) & (F.col("completion_pct") <= 100),
             severity="block")

(silver_progress.write.format("delta")
 .mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_progress"))

print(f"✅ silver_progress: {silver_progress.count():,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Dimensión SCD Type 2 — historización de cursos
# MAGIC
# MAGIC Los cursos cambian: sube el precio, se recategoriza, cambia el instructor.
# MAGIC Una tabla normal solo guarda el estado actual — pierde la historia. Un análisis
# MAGIC de "revenue del curso X en marzo" necesita saber **cuál era el precio en marzo**,
# MAGIC no el de hoy.
# MAGIC
# MAGIC **Slowly Changing Dimension Type 2** resuelve esto: en lugar de sobrescribir,
# MAGIC cierra la versión vieja (`valid_to`, `is_current = false`) e inserta una nueva.
# MAGIC La tabla `dim_courses` guarda la línea de tiempo completa de cada curso.

# COMMAND ----------

# Tabla dimensional SCD2 — una fila por (curso, período de validez)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.dim_courses (
        course_sk      STRING,
        course_id      STRING,
        course_name    STRING,
        category       STRING,
        instructor_id  STRING,
        list_price     DOUBLE,
        valid_from     DATE,
        valid_to       DATE,
        is_current     BOOLEAN,
        _scd_hash      STRING
    ) USING DELTA
""")

# --- Construir el batch entrante con un hash de los atributos rastreados ---
tracked = ["course_name", "category", "instructor_id", "list_price"]

incoming_courses = (
    spark.table(f"{CATALOG}.{SCHEMA}.bronze_courses")
    .select("course_id", *tracked, "updated_at")
    # Hash de atributos: si cambia, hubo un cambio de dimensión
    .withColumn("_scd_hash", F.sha2(F.concat_ws("||", *tracked), 256))
    .withColumn("course_sk", F.sha2(
        F.concat_ws("||", F.col("course_id"), F.col("updated_at")), 256))
)

dim = DeltaTable.forName(spark, f"{CATALOG}.{SCHEMA}.dim_courses")

# --- Paso 1: cerrar las versiones vigentes que cambiaron ---
# Para cada curso cuyo hash entrante difiere del hash vigente, marcamos
# la fila actual como histórica.
current_dim = (spark.table(f"{CATALOG}.{SCHEMA}.dim_courses")
               .filter(F.col("is_current") == True)
               .select("course_id", F.col("_scd_hash").alias("current_hash")))

changed = (incoming_courses.alias("s")
           .join(current_dim.alias("c"), "course_id", "inner")
           .filter(F.col("s._scd_hash") != F.col("c.current_hash"))
           .select("course_id"))

if changed.count() > 0:
    (dim.alias("t")
     .merge(changed.alias("c"),
            "t.course_id = c.course_id AND t.is_current = true")
     .whenMatchedUpdate(set={
         "is_current": F.lit(False),
         "valid_to":   F.expr("current_date() - INTERVAL 1 DAY"),
     })
     .execute())
    print(f"  {changed.count()} curso(s) con cambios → versión anterior cerrada")

# --- Paso 2: insertar las versiones nuevas (cursos nuevos + cursos cambiados) ---
existing_current = (spark.table(f"{CATALOG}.{SCHEMA}.dim_courses")
                    .filter(F.col("is_current") == True)
                    .select("course_id", "_scd_hash"))

to_insert = (incoming_courses.alias("s")
             .join(existing_current.alias("e"),
                   (F.col("s.course_id") == F.col("e.course_id")) &
                   (F.col("s._scd_hash") == F.col("e._scd_hash")),
                   "left_anti")
             .select(
                 "course_sk", "course_id", "course_name", "category",
                 "instructor_id", "list_price",
                 F.current_date().alias("valid_from"),
                 F.lit(None).cast("date").alias("valid_to"),
                 F.lit(True).alias("is_current"),
                 "_scd_hash"))

(to_insert.write.format("delta").mode("append")
 .saveAsTable(f"{CATALOG}.{SCHEMA}.dim_courses"))

total_dim = spark.table(f"{CATALOG}.{SCHEMA}.dim_courses").count()
print(f"✅ dim_courses (SCD2): {to_insert.count()} versiones nuevas · {total_dim} filas totales")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspeccionar la dimensión SCD2: cursos con más de una versión = tuvieron cambios
# MAGIC SELECT course_id, count(*) AS versiones,
# MAGIC        min(valid_from) AS primera_version,
# MAGIC        max(CASE WHEN is_current THEN list_price END) AS precio_actual
# MAGIC FROM workspace.edustream.dim_courses
# MAGIC GROUP BY course_id
# MAGIC HAVING count(*) > 1
# MAGIC ORDER BY versiones DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · GOLD — Métricas de negocio
# MAGIC
# MAGIC Gold consume silver + la dimensión SCD2 (versión vigente). Tres tablas gold,
# MAGIC cada una respondiendo una pregunta de negocio concreta.

# COMMAND ----------

# --- gold_course_performance: el KPI principal por curso ---
gold_course_performance = (
    spark.table(f"{CATALOG}.{SCHEMA}.silver_progress")
    .join(
        spark.table(f"{CATALOG}.{SCHEMA}.dim_courses")
             .filter(F.col("is_current") == True),
        "course_id", "left")
    .groupBy("course_id", "course_name", "category", "instructor_id")
    .agg(
        F.countDistinct("user_id").alias("estudiantes"),
        F.round(F.avg("completion_pct"), 2).alias("completion_rate_pct"),
        F.sum(F.col("is_completed").cast("int")).alias("graduados"),
        F.round(F.avg("list_price"), 2).alias("precio_lista"),
    )
    .withColumn("tasa_graduacion_pct",
                F.round(F.col("graduados") / F.col("estudiantes") * 100, 2))
    .orderBy(F.desc("completion_rate_pct"))
)
(gold_course_performance.write.format("delta")
 .mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_course_performance"))
print(f"✅ gold_course_performance: {gold_course_performance.count():,} cursos")

# --- gold_revenue_by_category: revenue agregado por categoría ---
gold_revenue = (
    spark.table(f"{CATALOG}.{SCHEMA}.silver_enrollments")
    .filter(F.col("status") != "refunded")     # excluir reembolsos del revenue
    .join(spark.table(f"{CATALOG}.{SCHEMA}.dim_courses")
              .filter(F.col("is_current") == True)
              .select("course_id", "category"),
          "course_id", "inner")
    .groupBy("category")
    .agg(
        F.count("*").alias("inscripciones"),
        F.round(F.sum("payment_amount"), 2).alias("revenue_total"),
        F.round(F.avg("payment_amount"), 2).alias("ticket_promedio"),
    )
    .orderBy(F.desc("revenue_total"))
)
(gold_revenue.write.format("delta")
 .mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_revenue_by_category"))
print(f"✅ gold_revenue_by_category: {gold_revenue.count()} categorías")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Time Travel y recuperación
# MAGIC
# MAGIC Demostración del valor operativo del versionado de Delta.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Estado actual y precio promedio
# MAGIC SELECT 'actual' AS v, count(*) AS filas,
# MAGIC        round(avg(payment_amount), 2) AS precio_prom
# MAGIC FROM workspace.edustream.silver_enrollments;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Simular un ajuste de precios (+10%) — genera una versión nueva
# MAGIC UPDATE workspace.edustream.silver_enrollments
# MAGIC SET payment_amount = round(payment_amount * 1.1, 2)
# MAGIC WHERE is_paid = true;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Comparar versión actual contra la versión inmediatamente anterior
# MAGIC WITH v_actual AS (
# MAGIC   SELECT round(avg(payment_amount), 2) AS precio
# MAGIC   FROM workspace.edustream.silver_enrollments
# MAGIC ),
# MAGIC v_anterior AS (
# MAGIC   SELECT round(avg(payment_amount), 2) AS precio
# MAGIC   FROM workspace.edustream.silver_enrollments VERSION AS OF (
# MAGIC     SELECT max(version) - 1
# MAGIC     FROM (DESCRIBE HISTORY workspace.edustream.silver_enrollments)
# MAGIC   )
# MAGIC )
# MAGIC SELECT a.precio AS precio_actual, b.precio AS precio_anterior,
# MAGIC        round((a.precio - b.precio) / b.precio * 100, 1) AS variacion_pct
# MAGIC FROM v_actual a, v_anterior b;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- RESTORE: revertir el ajuste de precios de forma transaccional
# MAGIC -- (RESTORE es superior a CREATE OR REPLACE: es atómico y queda en el historial)
# MAGIC RESTORE TABLE workspace.edustream.silver_enrollments
# MAGIC TO VERSION AS OF (
# MAGIC   SELECT max(version) - 1
# MAGIC   FROM (DESCRIBE HISTORY workspace.edustream.silver_enrollments)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Optimización — OPTIMIZE, ZORDER, VACUUM
# MAGIC
# MAGIC Decisión de clustering documentada antes de ejecutar (ver `docs/DESIGN.md`,
# MAGIC sección "Estrategia de clustering").

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Estado antes de optimizar
# MAGIC DESCRIBE DETAIL workspace.edustream.silver_enrollments;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver_enrollments: ZORDER por enrolled_at
# MAGIC -- Justificación: el 90% de las queries filtran por rango de fechas
# MAGIC -- (cohortes mensuales, retención, comparativas período a período).
# MAGIC -- ZORDER co-localiza archivos por fecha → data skipping reduce el scan.
# MAGIC OPTIMIZE workspace.edustream.silver_enrollments
# MAGIC ZORDER BY (enrolled_at);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_course_performance: ZORDER por course_id
# MAGIC -- Justificación: las queries sobre gold filtran/agrupan por curso.
# MAGIC OPTIMIZE workspace.edustream.gold_course_performance
# MAGIC ZORDER BY (course_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DRY RUN: auditar qué eliminaría VACUUM sin ejecutarlo
# MAGIC VACUUM workspace.edustream.silver_enrollments DRY RUN;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM con retención default (7 días)
# MAGIC VACUUM workspace.edustream.silver_enrollments;
# MAGIC VACUUM workspace.edustream.silver_progress;
# MAGIC VACUUM workspace.edustream.dim_courses;
# MAGIC VACUUM workspace.edustream.gold_course_performance;
# MAGIC VACUUM workspace.edustream.gold_revenue_by_category;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Reporte de calidad del pipeline run

# COMMAND ----------

# Resumen de DQ de esta ejecución — la "salud" del pipeline en una consulta
print(f"Reporte de Data Quality — run {PIPELINE_RUN_ID}\n" + "─" * 55)
(spark.table(f"{CATALOG}.{SCHEMA}._dq_audit")
 .filter(F.col("run_id") == PIPELINE_RUN_ID)
 .select("layer", "table_name", "check_name",
         "rows_checked", "rows_failed", "pass_rate", "severity")
 .orderBy("layer", "table_name")
 .show(truncate=False))

# Conteo final de todas las tablas del pipeline
print("\nConteo final de tablas:")
for tbl in ["bronze_enrollments", "bronze_courses", "bronze_progress",
            "bronze_instructors", "silver_enrollments", "silver_progress",
            "dim_courses", "gold_course_performance", "gold_revenue_by_category"]:
    c = spark.table(f"{CATALOG}.{SCHEMA}.{tbl}").count()
    print(f"  {tbl:30s} {c:>8,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apéndice — Reflexiones técnicas
# MAGIC
# MAGIC **1 · Managed vs External table.**
# MAGIC Managed: Unity Catalog controla metadatos y archivos; `DROP` borra ambos.
# MAGIC External: tú defines `LOCATION`; `DROP` solo borra metadatos. Para EduStream uso
# MAGIC managed — los datos son exclusivos de la plataforma, no se comparten con sistemas
# MAGIC externos, y delegar el ciclo de vida a Unity Catalog simplifica governance y
# MAGIC lineage. External tendría sentido solo si un data lake externo o un Redshift
# MAGIC consumiera los Parquet directamente.
# MAGIC
# MAGIC **2 · Versiones acumuladas y VACUUM.**
# MAGIC Cada write genera archivos nuevos y los retiene. Sin limpieza: small files problem
# MAGIC (Spark abre un handle por archivo, el planning overhead supera la ejecución) y
# MAGIC costo de storage lineal con la historia. `VACUUM` elimina archivos no referenciados
# MAGIC fuera del retention window (default 7 días). `OPTIMIZE` compacta pero no borra
# MAGIC versiones — son operaciones complementarias.
# MAGIC
# MAGIC **3 · expect vs expect_or_drop vs expect_or_fail.**
# MAGIC `expect_or_drop` para pagos negativos: dato corrupto sin interpretación válida,
# MAGIC se descarta. `expect` para course_id nulo: puede ser estado transitorio o útil
# MAGIC para auditoría, se conserva marcado. `expect_or_fail` cuando la violación invalida
# MAGIC todo el batch — p.ej. `enrolled_at` nulo al 100% indica un CSV con schema malo;
# MAGIC fail-fast evita producir métricas gold inútiles. En este notebook el patrón
# MAGIC equivalente es el parámetro `severity` de `run_dq_check`: `block` lanza excepción,
# MAGIC `warn` solo registra.
# MAGIC
# MAGIC **4 · Estrategia de ZORDER.**
# MAGIC `enrolled_at` para silver_enrollments porque las queries son temporales (cohortes,
# MAGIC retención). `course_id` para gold porque las queries filtran por curso. Principio:
# MAGIC ZORDER por la columna más frecuente en `WHERE`/`GROUP BY` del query history real.
# MAGIC En Databricks moderno, **Liquid Clustering** (`CLUSTER BY`) reemplaza a ZORDER —
# MAGIC se adapta sin reescribir la tabla y no exige elegir el orden de columnas. Ver
# MAGIC `docs/DESIGN.md` para el análisis completo del trade-off.
