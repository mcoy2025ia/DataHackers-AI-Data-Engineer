"""
pipeline.py — Orquestador Principal del Pipeline ETL Olist
===========================================================
Punto de entrada único. Coordina las capas Extract → Transform → Load
y gestiona el logging centralizado y el manejo de errores a nivel pipeline.

Uso:
    python pipeline.py
"""

import logging
import sys
import time
from pathlib import Path

# ──────────────────────────────────────────────
# Configuración de logging (debe hacerse ANTES de importar módulos src)
# ──────────────────────────────────────────────

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    datefmt=LOG_DATE_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log", encoding="utf-8"),
    ],
)

logger = logging.getLogger("pipeline")

# ──────────────────────────────────────────────
# Importaciones del proyecto
# ──────────────────────────────────────────────

from src.extract import fetch_brl_to_usd_rate, load_order_items, load_orders
from src.load import load_to_duckdb, query_revenue_by_month
from src.transform import apply_business_rules, clean_data, merge_datasets, split_quality


# ──────────────────────────────────────────────
# Orquestador
# ──────────────────────────────────────────────

def run_pipeline() -> None:
    """
    Ejecuta el pipeline ETL completo en el orden correcto:

      1. EXTRACT  → Carga CSVs y obtiene tasa de cambio de la API.
      2. TRANSFORM → Merge, limpieza, reglas de negocio y control de calidad.
      3. LOAD     → Persiste dim_orders y fact_ventas_usd en DuckDB.
      4. INSIGHT  → Imprime el revenue mensual en USD.

    En caso de error irrecuperable, loguea la excepción y termina con
    código de salida 1 para que el orquestador externo (Airflow, CI/CD)
    detecte el fallo.
    """
    start = time.perf_counter()
    logger.info("=" * 60)
    logger.info("PIPELINE ETL OLIST — INICIO")
    logger.info("=" * 60)

    # ── PASO 1: EXTRACT ─────────────────────────────────────────
    logger.info("▶ [1/4] EXTRACCIÓN")
    try:
        df_orders      = load_orders()
        df_items       = load_order_items()
        brl_to_usd     = fetch_brl_to_usd_rate()
    except FileNotFoundError as exc:
        logger.critical("Archivo fuente no encontrado: %s", exc)
        sys.exit(1)

    # ── PASO 2: TRANSFORM ───────────────────────────────────────
    logger.info("▶ [2/4] TRANSFORMACIÓN")
    df_merged  = merge_datasets(orders=df_orders, items=df_items)
    df_clean   = clean_data(df_merged)
    df_enriched = apply_business_rules(df_clean, brl_to_usd=brl_to_usd)

    logger.info("▶ [3/4] CONTROL DE CALIDAD")
    df_valid, df_rejected = split_quality(df_enriched)

    if df_valid.empty:
        logger.critical("No quedaron registros válidos tras el control de calidad. Abortando.")
        sys.exit(1)

    # ── PASO 3: LOAD ────────────────────────────────────────────
    logger.info("▶ [4/4] CARGA")
    db_path = load_to_duckdb(df_valid)

    # ── INSIGHT: Revenue mensual en USD ─────────────────────────
    logger.info("📊 Generando insight: Revenue mensual en USD")
    try:
        df_revenue = query_revenue_by_month(db_path)
        logger.info("\n%s", df_revenue.to_string(index=False))
    except Exception as exc:  # noqa: BLE001
        logger.warning("No se pudo generar el insight de revenue: %s", exc)

    # ── RESUMEN ─────────────────────────────────────────────────
    elapsed = time.perf_counter() - start
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETADO en %.2f segundos", elapsed)
    logger.info("  Registros procesados : %d", len(df_valid))
    logger.info("  Registros rechazados : %d", len(df_rejected))
    logger.info("  Base de datos        : %s", db_path)
    logger.info("=" * 60)


# ──────────────────────────────────────────────
# Entry-point
# ──────────────────────────────────────────────

if __name__ == "__main__":
    run_pipeline()
