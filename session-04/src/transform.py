"""
transform.py — Capa de Transformación del Pipeline ETL Olist
=============================================================
Responsabilidades:
  - Merge de órdenes e ítems por order_id
  - Limpieza: conversión de fechas, manejo de nulos, deduplicación
  - Lógica de negocio: total_brl, total_usd, dias_para_entrega
  - Calidad de datos: separar registros rechazados con motivo
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Constantes
# ──────────────────────────────────────────────
REJECTED_DIR = Path(__file__).parents[1] / "data" / "rejected"

DATE_COLUMNS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


# ──────────────────────────────────────────────
# Funciones internas (privadas)
# ──────────────────────────────────────────────

def _parse_dates(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Convierte columnas de texto ISO 8601 a dtype datetime64."""
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            n_invalid = df[col].isna().sum()
            if n_invalid:
                logger.debug("Columna '%s': %d valores no parseables → NaT", col, n_invalid)
    return df


def _handle_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estrategia de nulos:
      - Fechas opcionales (approved_at, carrier_date): se mantienen como NaT.
      - order_id, product_id, price, freight_value: no deben ser nulos.
    """
    critical_cols = ["order_id", "product_id", "price", "freight_value"]
    before = len(df)
    df = df.dropna(subset=[c for c in critical_cols if c in df.columns])
    dropped = before - len(df)
    if dropped:
        logger.warning("Eliminadas %d filas con nulos en columnas críticas", dropped)
    return df


def _deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina filas completamente duplicadas."""
    before = len(df)
    df = df.drop_duplicates()
    dropped = before - len(df)
    if dropped:
        logger.info("Eliminados %d registros duplicados", dropped)
    return df


# ──────────────────────────────────────────────
# Funciones públicas
# ──────────────────────────────────────────────

def merge_datasets(
    orders: pd.DataFrame,
    items: pd.DataFrame,
) -> pd.DataFrame:
    """
    Realiza un LEFT JOIN de órdenes e ítems por order_id.

    Args:
        orders: DataFrame de pedidos (olist_orders_dataset).
        items:  DataFrame de ítems (olist_order_items_dataset).

    Returns:
        DataFrame fusionado con todos los campos de ambas fuentes.
    """
    logger.info(
        "Mergeando órdenes (%d) con ítems (%d) por order_id…",
        len(orders),
        len(items),
    )
    merged = pd.merge(orders, items, on="order_id", how="left")
    logger.info("Resultado del merge: %d filas", len(merged))
    return merged


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica limpieza estándar sobre el DataFrame fusionado:
      1. Conversión de fechas ISO → datetime.
      2. Manejo de nulos en campos críticos.
      3. Eliminación de duplicados exactos.

    Args:
        df: DataFrame fusionado sin limpiar.

    Returns:
        DataFrame limpio.
    """
    logger.info("Iniciando limpieza de datos…")
    df = _parse_dates(df, DATE_COLUMNS)
    df = _handle_nulls(df)
    df = _deduplicate(df)
    logger.info("Limpieza completada → %d filas restantes", len(df))
    return df


def apply_business_rules(df: pd.DataFrame, brl_to_usd: float) -> pd.DataFrame:
    """
    Calcula métricas de negocio sobre el DataFrame limpio:
      - total_brl       = price * quantity (proxy: 1 ítem por fila)
      - total_usd       = total_brl * brl_to_usd
      - dias_para_entrega = order_delivered_customer_date - order_purchase_timestamp

    Args:
        df:           DataFrame limpio.
        brl_to_usd:   Tasa de cambio BRL → USD (obtenida de la API).

    Returns:
        DataFrame con columnas calculadas añadidas.
    """
    logger.info("Aplicando reglas de negocio (tasa BRL→USD: %.6f)…", brl_to_usd)

    # Cada fila representa 1 ítem; price ya es el precio unitario en BRL
    df = df.copy()
    df["total_brl"] = df["price"].fillna(0) + df["freight_value"].fillna(0)
    df["total_usd"] = (df["total_brl"] * brl_to_usd).round(4)

    # Días de entrega (puede ser NaT si la orden no fue entregada)
    if (
        "order_delivered_customer_date" in df.columns
        and "order_purchase_timestamp" in df.columns
    ):
        df["dias_para_entrega"] = (
            df["order_delivered_customer_date"] - df["order_purchase_timestamp"]
        ).dt.days
    else:
        df["dias_para_entrega"] = pd.NA

    logger.info("Columnas calculadas: total_brl, total_usd, dias_para_entrega")
    return df


def split_quality(
    df: pd.DataFrame,
    rejected_dir: Path = REJECTED_DIR,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Separa registros válidos de rechazados según reglas de calidad:
      - Rechazado si total_brl <= 0
      - Rechazado si order_purchase_timestamp es NaT (fecha inválida)

    Exporta los rechazados a data/rejected/rejected_records.csv con
    la columna adicional `rejection_reason`.

    Args:
        df:           DataFrame con reglas de negocio aplicadas.
        rejected_dir: Ruta al directorio de rechazados.

    Returns:
        Tuple (df_valid, df_rejected).
    """
    logger.info("Evaluando calidad de datos…")
    df = df.copy()

    # Máscara de condiciones de rechazo
    mask_amount = df["total_brl"] <= 0
    mask_date   = df["order_purchase_timestamp"].isna()

    # Construir columna de motivo (puede haber múltiples motivos)
    reasons: pd.Series = pd.Series("", index=df.index)
    reasons[mask_amount] += "monto_invalido;"
    reasons[mask_date]   += "fecha_compra_invalida;"

    mask_rejected = mask_amount | mask_date

    df_rejected = df[mask_rejected].copy()
    df_rejected["rejection_reason"] = reasons[mask_rejected].str.rstrip(";")
    df_valid    = df[~mask_rejected].copy()

    logger.info(
        "Calidad: %d válidos | %d rechazados",
        len(df_valid),
        len(df_rejected),
    )

    # Exportar rechazados
    rejected_dir.mkdir(parents=True, exist_ok=True)
    out_path = rejected_dir / "rejected_records.csv"
    df_rejected.to_csv(out_path, index=False)
    logger.info("Archivo de rechazados generado en: %s", out_path)


    return df_valid, df_rejected
