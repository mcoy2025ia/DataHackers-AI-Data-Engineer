"""
load.py — Capa de Carga del Pipeline ETL Olist
===============================================
Responsabilidades:
  - Crear / actualizar ecommerce.duckdb en data/processed/
  - Materializar dim_orders (dimensión de pedidos)
  - Materializar fact_ventas_usd (tabla de hechos de ventas)
"""

import logging
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Constantes
# ──────────────────────────────────────────────
PROCESSED_DIR = Path(__file__).parents[1] / "data" / "processed"
DB_NAME = "ecommerce.duckdb"

# Columnas que componen cada tabla destino
DIM_ORDERS_COLS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "dias_para_entrega",
]

FACT_VENTAS_COLS = [
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "price",
    "freight_value",
    "total_brl",
    "total_usd",
    "order_purchase_timestamp",
]


# ──────────────────────────────────────────────
# Funciones internas
# ──────────────────────────────────────────────

def _select_existing_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Filtra sólo las columnas que existen en el DataFrame (seguridad ante esquemas parciales)."""
    available = [c for c in cols if c in df.columns]
    missing = set(cols) - set(available)
    if missing:
        logger.debug("Columnas no encontradas en el DataFrame (se omiten): %s", missing)
    return df[available]


def _write_table(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    table_name: str,
) -> None:
    """
    Escribe un DataFrame en DuckDB usando CREATE OR REPLACE TABLE.

    Args:
        conn:       Conexión activa a DuckDB.
        df:         DataFrame a persistir.
        table_name: Nombre de la tabla destino.
    """
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")  # noqa: S608
    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]  # noqa: S608
    logger.info("Tabla '%s' creada → %d registros", table_name, count)


# ──────────────────────────────────────────────
# Función pública principal
# ──────────────────────────────────────────────

def load_to_duckdb(
    df_valid: pd.DataFrame,
    processed_dir: Path = PROCESSED_DIR,
    db_name: str = DB_NAME,
) -> Path:
    """
    Carga los datos transformados en un archivo DuckDB local.

    Crea dos tablas:
      - dim_orders      → dimensión descriptiva de cada pedido.
      - fact_ventas_usd → tabla de hechos con métricas financieras.

    Args:
        df_valid:      DataFrame con registros válidos (post-transformación).
        processed_dir: Directorio donde se almacena el archivo .duckdb.
        db_name:       Nombre del archivo DuckDB.

    Returns:
        Path absoluto al archivo DuckDB generado.
    """
    processed_dir.mkdir(parents=True, exist_ok=True)
    db_path = processed_dir / db_name

    logger.info("Conectando a DuckDB: %s", db_path)
    conn = duckdb.connect(str(db_path))

    try:
        # ── dim_orders ──────────────────────────────
        df_dim = _select_existing_cols(df_valid, DIM_ORDERS_COLS).drop_duplicates(
            subset=["order_id"]
        )
        logger.info("Cargando dim_orders (%d pedidos únicos)…", len(df_dim))
        df = df_dim  # alias para el contexto SQL de DuckDB
        _write_table(conn, df, "dim_orders")

        # ── fact_ventas_usd ─────────────────────────
        df_fact = _select_existing_cols(df_valid, FACT_VENTAS_COLS)
        logger.info("Cargando fact_ventas_usd (%d ítems de venta)…", len(df_fact))
        df = df_fact  # alias para el contexto SQL de DuckDB
        _write_table(conn, df, "fact_ventas_usd")

        conn.commit()
        logger.info("✔ Carga completada en: %s", db_path)

    finally:
        conn.close()

    return db_path


def query_revenue_by_month(db_path: Path) -> pd.DataFrame:
    """
    Consulta el revenue total en USD agrupado por mes.

    Útil para generar el insight de revenue mensual en el README.

    Args:
        db_path: Ruta al archivo DuckDB generado.

    Returns:
        DataFrame con columnas [mes, revenue_usd].
    """
    logger.info("Consultando revenue mensual desde: %s", db_path)
    conn = duckdb.connect(str(db_path), read_only=True)

    try:
        df_result = conn.execute(
            """
            SELECT
                strftime(order_purchase_timestamp, '%Y-%m') AS mes,
                ROUND(SUM(total_usd), 2)                    AS revenue_usd
            FROM fact_ventas_usd
            WHERE order_purchase_timestamp IS NOT NULL
            GROUP BY mes
            ORDER BY mes
            """
        ).df()
    finally:
        conn.close()

    return df_result
