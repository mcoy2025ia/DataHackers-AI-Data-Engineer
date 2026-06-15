"""
Tests de validación del pipeline EduStream
===========================================
Valida la lógica de transformación contra datos sintéticos usando Spark local.

Los tests prueban la LÓGICA de transformación (deduplicación, limpieza, SCD2,
agregación), que es independiente del motor de almacenamiento. El comportamiento
del MERGE de Delta se valida en Databricks; aquí validamos la equivalencia
funcional del upsert (merge = última versión gana, sin duplicados).

Ejecutar:
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 python -m pytest tests/ -v
"""
import pytest
from pyspark.sql import SparkSession, functions as F, Window


@pytest.fixture(scope="session")
def spark():
    sess = (SparkSession.builder
            .appName("edustream-tests")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .getOrCreate())
    sess.sparkContext.setLogLevel("ERROR")
    yield sess
    sess.stop()


@pytest.fixture
def raw_enrollments(spark):
    """Enrollments con duplicados, nulos y negativos."""
    return spark.createDataFrame([
        ("ENR001", "USR1", "CRS1", 49.99, "2025-03-01", "active"),
        ("ENR001", "USR1", "CRS1", 49.99, "2025-03-01", "active"),
        ("ENR002", "USR2", "CRS2", None,  "2025-03-02", "active"),
        ("ENR003", "USR3", "CRS3", -20.0, "2025-03-03", "active"),
        ("ENR004", "USR4", None,   29.99, "2025-03-04", "active"),
    ], "enrollment_id string, user_id string, course_id string, "
       "payment_amount double, enrolled_at string, status string")


def test_deduplicacion(spark, raw_enrollments):
    df = raw_enrollments.withColumn("_ing", F.monotonically_increasing_id())
    w = Window.partitionBy("enrollment_id").orderBy(F.col("_ing").desc())
    deduped = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1)
    assert deduped.count() == 4
    assert deduped.select("enrollment_id").distinct().count() == 4


def test_nulos_a_cero(spark, raw_enrollments):
    cleaned = raw_enrollments.withColumn(
        "payment_amount", F.coalesce(F.col("payment_amount"), F.lit(0.0)))
    assert cleaned.filter(F.col("enrollment_id") == "ENR002").first()["payment_amount"] == 0.0
    assert cleaned.filter(F.col("payment_amount").isNull()).count() == 0


def test_descarta_negativos(spark, raw_enrollments):
    valid = raw_enrollments.filter(F.col("payment_amount") >= 0)
    assert "ENR003" not in {r["enrollment_id"] for r in valid.collect()}


def test_is_paid_derivado(spark, raw_enrollments):
    df = (raw_enrollments
          .withColumn("payment_amount", F.coalesce(F.col("payment_amount"), F.lit(0.0)))
          .withColumn("is_paid", F.col("payment_amount") > 0))
    assert df.filter(F.col("enrollment_id") == "ENR002").first()["is_paid"] is False
    assert df.filter(F.col("enrollment_id") == "ENR001").first()["is_paid"] is True


def test_filtro_total_lessons_cero(spark):
    progress = spark.createDataFrame([
        ("USR1", "CRS1", 5, 10), ("USR2", "CRS2", 0, 0), ("USR3", "CRS3", 8, 8),
    ], "user_id string, course_id string, completed_lessons int, total_lessons int")
    filtered = progress.filter(F.col("total_lessons") > 0)
    assert filtered.count() == 2
    assert "CRS2" not in {r["course_id"] for r in filtered.collect()}


def test_completion_pct_en_rango(spark):
    progress = (spark.createDataFrame([
        ("USR1", "CRS1", 5, 10), ("USR2", "CRS2", 10, 10), ("USR3", "CRS3", 0, 20),
    ], "user_id string, course_id string, completed_lessons int, total_lessons int")
        .withColumn("completion_pct",
                    F.round(F.col("completed_lessons") / F.col("total_lessons") * 100, 2)))
    rows = progress.collect()
    assert all(0 <= r["completion_pct"] <= 100 for r in rows)
    assert next(r for r in rows if r["course_id"] == "CRS2")["completion_pct"] == 100.0


def test_scd2_hash_determinista(spark):
    tracked = ["course_name", "category", "list_price"]
    base = spark.createDataFrame([
        ("CRS1", "Spark", "Data Eng", 49.99), ("CRS2", "ML 101", "ML", 79.99),
    ], "course_id string, course_name string, category string, list_price double")
    h1 = {r["course_id"]: r["_hash"] for r in base.withColumn(
        "_hash", F.sha2(F.concat_ws("||", *tracked), 256)).collect()}
    h2 = {r["course_id"]: r["_hash"] for r in base.withColumn(
        "_hash", F.sha2(F.concat_ws("||", *tracked), 256)).collect()}
    assert h1 == h2


def test_scd2_detecta_cambios(spark):
    tracked = ["course_name", "category", "list_price"]
    original = spark.createDataFrame([("CRS1", "Spark", "Data Eng", 49.99)],
        "course_id string, course_name string, category string, list_price double")
    changed = spark.createDataFrame([("CRS1", "Spark", "Data Eng", 59.99)],
        "course_id string, course_name string, category string, list_price double")
    h_orig = original.withColumn("_hash", F.sha2(F.concat_ws("||", *tracked), 256)).first()["_hash"]
    h_changed = changed.withColumn("_hash", F.sha2(F.concat_ws("||", *tracked), 256)).first()["_hash"]
    assert h_orig != h_changed


def test_upsert_idempotente(spark):
    target = spark.createDataFrame([("ENR001", 49.99), ("ENR002", 0.0)],
        "enrollment_id string, payment_amount double")
    batch = spark.createDataFrame([("ENR001", 49.99), ("ENR002", 0.0), ("ENR003", 29.99)],
        "enrollment_id string, payment_amount double")

    def merge(tgt, src):
        return (tgt.alias("t").join(src.alias("s"), "enrollment_id", "full_outer")
                .select("enrollment_id",
                        F.coalesce(F.col("s.payment_amount"),
                                   F.col("t.payment_amount")).alias("payment_amount")))

    r1 = merge(target, batch)
    r3 = merge(merge(r1, batch), batch)
    assert r1.count() == r3.count() == 3
    rows_1 = sorted((r["enrollment_id"], r["payment_amount"]) for r in r1.collect())
    rows_3 = sorted((r["enrollment_id"], r["payment_amount"]) for r in r3.collect())
    assert rows_1 == rows_3


def test_revenue_excluye_refunds(spark):
    enrollments = spark.createDataFrame([
        ("ENR001", "CRS1", 49.99, "active"),
        ("ENR002", "CRS1", 49.99, "refunded"),
        ("ENR003", "CRS1", 29.99, "active"),
    ], "enrollment_id string, course_id string, payment_amount double, status string")
    revenue = (enrollments.filter(F.col("status") != "refunded")
               .agg(F.sum("payment_amount").alias("total")).first()["total"])
    assert revenue == pytest.approx(79.98)
