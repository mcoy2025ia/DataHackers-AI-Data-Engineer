"""
extract.py — Capa de Extracción del Pipeline ETL Olist
=======================================================
Responsabilidades:
  - Leer archivos CSV desde data/raw/
  - Consumir la API de tasas de cambio (BRL → USD)
  - Retornar DataFrames y tasa de cambio limpia al orquestador
"""

import logging
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Constantes
# ──────────────────────────────────────────────
RAW_DIR = Path(__file__).parents[1] / "data" / "raw"
EXCHANGE_API_URL = "https://open.er-api.com/v6/latest/BRL"
REQUEST_TIMEOUT_SECONDS = 10


# ──────────────────────────────────────────────
# Funciones públicas
# ──────────────────────────────────────────────

def load_orders(raw_dir: Path = RAW_DIR) -> pd.DataFrame:
    """
    Lee olist_orders_dataset.csv desde el directorio raw.

    Args:
        raw_dir: Ruta al directorio data/raw/.

    Returns:
        DataFrame con los pedidos crudos de Olist.

    Raises:
        FileNotFoundError: Si el archivo CSV no existe en la ruta esperada.
    """
    path = raw_dir / "olist_orders_dataset.csv"
    if not path.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {path}")

    logger.info("Leyendo órdenes desde: %s", path)
    df = pd.read_csv(path)
    logger.info("Órdenes cargadas → %d filas, %d columnas", *df.shape)
    return df


def load_order_items(raw_dir: Path = RAW_DIR) -> pd.DataFrame:
    """
    Lee olist_order_items_dataset.csv desde el directorio raw.

    Args:
        raw_dir: Ruta al directorio data/raw/.

    Returns:
        DataFrame con los ítems de pedido crudos de Olist.

    Raises:
        FileNotFoundError: Si el archivo CSV no existe en la ruta esperada.
    """
    path = raw_dir / "olist_order_items_dataset.csv"
    if not path.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {path}")

    logger.info("Leyendo ítems de pedido desde: %s", path)
    df = pd.read_csv(path)
    logger.info("Ítems cargados → %d filas, %d columnas", *df.shape)
    return df


def fetch_brl_to_usd_rate(api_url: str = EXCHANGE_API_URL) -> float:
    """
    Obtiene la tasa de cambio BRL → USD desde la API open.er-api.com.

    Implementa manejo de fallos de red con bloque try-except explícito.
    En caso de error, usa una tasa de fallback conocida y loguea una advertencia.

    Args:
        api_url: URL del endpoint de la API de tasas de cambio.

    Returns:
        Tasa de cambio BRL/USD como float (ej: 0.19).
    """
    FALLBACK_RATE = 0.19  # Tasa de contingencia si la API falla

    try:
        logger.info("Consultando API de tasas de cambio: %s", api_url)
        response = requests.get(api_url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()

        payload: dict = response.json()

        if payload.get("result") != "success":
            raise ValueError(f"La API reportó resultado no exitoso: {payload.get('result')}")

        usd_rate: float = payload["rates"]["USD"]
        logger.info("Tasa BRL → USD obtenida: %.6f", usd_rate)
        return usd_rate

    except requests.exceptions.Timeout:
        logger.warning(
            "Timeout al conectar con la API (%ds). Usando tasa de fallback: %.4f",
            REQUEST_TIMEOUT_SECONDS,
            FALLBACK_RATE,
        )
        return FALLBACK_RATE

    except requests.exceptions.ConnectionError:
        logger.warning(
            "Error de conexión de red. Usando tasa de fallback: %.4f", FALLBACK_RATE
        )
        return FALLBACK_RATE

    except requests.exceptions.HTTPError as exc:
        logger.warning(
            "Error HTTP %s de la API. Usando tasa de fallback: %.4f",
            exc.response.status_code,
            FALLBACK_RATE,
        )
        return FALLBACK_RATE

    except (KeyError, ValueError) as exc:
        logger.warning(
            "Respuesta inesperada de la API: %s. Usando tasa de fallback: %.4f",
            exc,
            FALLBACK_RATE,
        )
        return FALLBACK_RATE
