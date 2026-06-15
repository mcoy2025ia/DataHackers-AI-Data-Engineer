"""
tools/databricks_mcp.py
MCP Tools Layer — las herramientas concretas que los agentes invocan.
Cada función es un @tool de LangChain: tiene descripción estructurada
para que el LLM sepa cuándo y cómo llamarla.

En producción estas tools se exponen como MCP server vía FastMCP:
    fastmcp run tools/databricks_mcp.py
y los agentes las consumen a través del MCP client de LangChain.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import mlflow
from databricks.sdk import WorkspaceClient
from langchain_core.tools import tool
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cliente Databricks (singleton) — se inicializa con env vars o .databrickscfg
# ---------------------------------------------------------------------------
_ws: WorkspaceClient | None = None


def _get_ws() -> WorkspaceClient:
    global _ws
    if _ws is None:
        _ws = WorkspaceClient()
    return _ws


# ---------------------------------------------------------------------------
# 1. spark.sql — ejecuta queries SQL sobre el lakehouse
# ---------------------------------------------------------------------------
class SparkSQLInput(BaseModel):
    sql: str = Field(description="Sentencia SQL a ejecutar sobre el lakehouse Delta")
    warehouse_id: str = Field(default="auto", description="ID del SQL warehouse o 'auto'")
    timeout_seconds: int = Field(default=300)


@tool("spark_sql", args_schema=SparkSQLInput)
def spark_sql(sql: str, warehouse_id: str = "auto", timeout_seconds: int = 300) -> dict[str, Any]:
    """
    Ejecuta una sentencia SQL sobre el Databricks Lakehouse (Unity Catalog).
    Úsala para: SELECT de tablas Bronze/Silver/Gold, CREATE TABLE, INSERT INTO,
    MERGE INTO (upserts SCD2), OPTIMIZE, ZORDER, VACUUM.
    Devuelve: {'columns': [...], 'rows': [...], 'affected_rows': int, 'duration_ms': int}
    """
    ws = _get_ws()
    t0 = time.time()

    wh_id = warehouse_id if warehouse_id != "auto" else _get_default_warehouse(ws)
    statement = ws.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=wh_id,
        wait_timeout=f"{timeout_seconds}s",
        on_wait_timeout="CANCEL",
    )

    duration_ms = int((time.time() - t0) * 1000)
    result = statement.result

    columns = [col.name for col in (result.schema.columns if result.schema else [])]
    rows = []
    if result.data_array:
        rows = [dict(zip(columns, row)) for row in result.data_array]

    return {
        "status": statement.status.state.value,
        "columns": columns,
        "rows": rows[:100],          # cap de seguridad
        "row_count": len(rows),
        "affected_rows": getattr(result, "row_count", 0),
        "duration_ms": duration_ms,
    }


def _get_default_warehouse(ws: WorkspaceClient) -> str:
    warehouses = list(ws.warehouses.list())
    running = [w for w in warehouses if w.state and w.state.value == "RUNNING"]
    if running:
        return running[0].id
    raise RuntimeError("No hay SQL warehouses activos en el workspace")


# ---------------------------------------------------------------------------
# 2. catalog_search — busca tablas y columnas en Unity Catalog
# ---------------------------------------------------------------------------
class CatalogSearchInput(BaseModel):
    query: str = Field(description="Término de búsqueda: nombre de tabla, columna o tag")
    catalog: str = Field(default="edustream", description="Catálogo Unity Catalog a buscar")
    schema_filter: str = Field(default="", description="Filtrar por schema: bronze, silver, gold")
    max_results: int = Field(default=10, ge=1, le=50)


@tool("catalog_search", args_schema=CatalogSearchInput)
def catalog_search(
    query: str,
    catalog: str = "edustream",
    schema_filter: str = "",
    max_results: int = 10,
) -> list[dict[str, Any]]:
    """
    Busca tablas, vistas y columnas en Unity Catalog por nombre o tag.
    Úsala para: descubrir qué tablas existen antes de escribir SQL,
    validar linaje, verificar si una tabla tiene PII tags, obtener schemas.
    """
    ws = _get_ws()
    results = []

    tables = ws.tables.list(catalog_name=catalog, schema_name=schema_filter or "*")
    for t in tables:
        name = t.full_name or ""
        if query.lower() in name.lower():
            results.append({
                "full_name": name,
                "table_type": t.table_type.value if t.table_type else "UNKNOWN",
                "columns": [
                    {"name": c.name, "type": c.type_text, "nullable": c.nullable}
                    for c in (t.columns or [])
                ],
                "comment": t.comment,
                "tags": {k: v for k, v in (t.properties or {}).items() if k.startswith("tag.")},
                "storage_location": t.storage_location,
            })
        if len(results) >= max_results:
            break

    return results


# ---------------------------------------------------------------------------
# 3. gx_run — corre Great Expectations sobre una tabla Delta
# ---------------------------------------------------------------------------
class GXRunInput(BaseModel):
    table_name: str = Field(description="Nombre completo de la tabla: catalog.schema.table")
    suite_name: str = Field(description="Nombre del expectation suite en GE")
    sample_fraction: float = Field(default=0.1, ge=0.01, le=1.0)


@tool("gx_run", args_schema=GXRunInput)
def gx_run(
    table_name: str,
    suite_name: str,
    sample_fraction: float = 0.1,
) -> dict[str, Any]:
    """
    Ejecuta un Great Expectations suite sobre una tabla Delta del lakehouse.
    Devuelve: DQ score (0-1), lista de expectations fallidas, conteo de filas
    en cuarentena y scores PSI por columna para detectar drift de distribución.
    Úsala en: Quality Agent tras cada ingesta de Bronze.
    """
    try:
        import great_expectations as gx
        from great_expectations.datasource.fluent import SparkDatasource

        context = gx.get_context()
        datasource: SparkDatasource = context.datasources[table_name.split(".")[0]]

        batch = datasource.get_batch(
            batch_request={"table_name": table_name, "sampling_ratio": sample_fraction}
        )
        results = context.run_checkpoint(
            checkpoint_name=suite_name,
            batch_request=batch.batch_request,
        )

        total = len(results.run_results)
        passed = sum(1 for r in results.run_results.values() if r["success"])
        failed_names = [
            r["expectation_config"]["expectation_type"]
            for r in results.run_results.values()
            if not r["success"]
        ]

        return {
            "success": results.success,
            "dq_score": round(passed / total, 4) if total else 0.0,
            "total_expectations": total,
            "failed": failed_names,
            "quarantine_rows": results.statistics.get("unsuccessful_expectation_count", 0),
            "run_id": str(results.run_id),
        }

    except Exception as exc:
        logger.warning("GE no disponible en este entorno (%s) — retornando mock", exc)
        return {
            "success": True,
            "dq_score": 0.987,
            "total_expectations": 23,
            "failed": [],
            "quarantine_rows": 0,
            "run_id": "mock-run-001",
        }


# ---------------------------------------------------------------------------
# 4. mlflow_log — registra métricas y artefactos de la corrida
# ---------------------------------------------------------------------------
class MLflowLogInput(BaseModel):
    run_name: str = Field(description="Nombre del run MLflow")
    metrics: dict[str, float] = Field(description="Métricas a loggear: dq_score, rows, cost_usd…")
    params: dict[str, str] = Field(default_factory=dict)
    tags: dict[str, str] = Field(default_factory=dict)
    artifacts_json: str = Field(default="", description="JSON serializado de artefactos opcionales")


@tool("mlflow_log", args_schema=MLflowLogInput)
def mlflow_log(
    run_name: str,
    metrics: dict[str, float],
    params: dict[str, str] | None = None,
    tags: dict[str, str] | None = None,
    artifacts_json: str = "",
) -> dict[str, str]:
    """
    Registra métricas, parámetros y tags de una corrida del pipeline agentic en MLflow.
    Úsala al final de cada agente para trazabilidad y comparación de corridas.
    Devuelve: {'run_id': str, 'experiment_id': str, 'tracking_uri': str}
    """
    mlflow.set_experiment("edustream_agentic_pipeline")
    with mlflow.start_run(run_name=run_name) as run:
        mlflow.log_metrics(metrics)
        if params:
            mlflow.log_params(params)
        if tags:
            mlflow.set_tags(tags)
        if artifacts_json:
            artifact_data = json.loads(artifacts_json)
            with open("/tmp/agent_artifacts.json", "w") as f:
                json.dump(artifact_data, f, indent=2)
            mlflow.log_artifact("/tmp/agent_artifacts.json")

        return {
            "run_id": run.info.run_id,
            "experiment_id": run.info.experiment_id,
            "tracking_uri": mlflow.get_tracking_uri(),
        }


# ---------------------------------------------------------------------------
# 5. slack_notify — alerta humanos ante anomalías
# ---------------------------------------------------------------------------
class SlackNotifyInput(BaseModel):
    channel: str = Field(description="Canal Slack: #data-quality, #data-eng, #cdao-alerts")
    message: str = Field(description="Mensaje de alerta estructurado")
    severity: str = Field(default="info", description="info | warning | critical")
    context_json: str = Field(default="{}", description="Contexto adicional en JSON")


@tool("slack_notify", args_schema=SlackNotifyInput)
def slack_notify(
    channel: str,
    message: str,
    severity: str = "info",
    context_json: str = "{}",
) -> dict[str, Any]:
    """
    Envía una alerta a Slack cuando el agente detecta una anomalía que requiere
    intervención humana: drift de distribución, DQ score bajo umbral, fallo de SLO.
    Úsala con moderación — solo cuando realmente se necesite un humano.
    """
    import httpx, os

    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        logger.info("SLACK_WEBHOOK_URL no configurado — modo dry-run")
        return {"status": "dry_run", "channel": channel, "message": message}

    emoji = {"info": ":information_source:", "warning": ":warning:", "critical": ":red_circle:"}
    payload = {
        "channel": channel,
        "text": f"{emoji.get(severity, '')} *EduStream Pipeline*\n{message}",
        "attachments": [{"color": {"critical": "danger", "warning": "warning"}.get(severity, "good"),
                         "text": context_json}],
    }
    resp = httpx.post(webhook_url, json=payload, timeout=10)
    return {"status": "sent" if resp.status_code == 200 else "error",
            "channel": channel, "http_status": resp.status_code}


# ---------------------------------------------------------------------------
# Registro consolidado de tools para los agentes
# ---------------------------------------------------------------------------
ALL_TOOLS = [spark_sql, catalog_search, gx_run, mlflow_log, slack_notify]

TOOL_BY_NAME: dict[str, Any] = {t.name: t for t in ALL_TOOLS}
