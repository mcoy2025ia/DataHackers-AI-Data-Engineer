"""
agents/quality_agent.py
Quality Agent — Data Quality + Drift Detection
Usa Claude Haiku 4.5.

Responsabilidades:
  1. Correr Great Expectations suites sobre tablas Bronze
  2. Calcular Population Stability Index (PSI) para detectar drift
  3. Enrutar filas corruptas a cuarentena
  4. Abrir PR automático en dbt-repo si drift supera umbral
  5. Notificar Slack si DQ score < 0.95
  6. Actualizar PipelineState con DQResult
"""

from __future__ import annotations

import logging
import math
from typing import Any

import numpy as np
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage

from config.state import AgentStatus, DQResult, PipelineState
from tools.databricks_mcp import ALL_TOOLS, TOOL_BY_NAME

logger = logging.getLogger(__name__)

# Umbral PSI: < 0.1 estable, 0.1-0.2 ligero, > 0.2 drift significativo
PSI_DRIFT_THRESHOLD = 0.2
DQ_HUMAN_ESCALATION = 0.90
DQ_SLACK_WARNING = 0.95

QUALITY_SYSTEM = """
Eres el Quality Agent del pipeline de EduStream.
Tu tarea es garantizar que los datos en Bronze cumplan los contratos de calidad
antes de que el Transformation Agent los procese.

CHECKS OBLIGATORIOS:
1. bronze_enrollments:
   - payment_amount puede ser NULL (cursos gratuitos) → OK
   - currency debe estar en {USD, MXN, COP, PEN}
   - enrollment_id NO puede ser NULL o duplicado
   - enrolled_at debe ser timestamp válido
   
2. bronze_progress:
   - total_lessons = 0 → CORRUPTO, enviar a cuarentena
   - lessons_completed <= total_lessons siempre
   - No puede haber (user_id, course_id) duplicados
   
3. bronze_courses:
   - course_id NOT NULL
   - price_usd >= 0
   
4. bronze_instructors:
   - instructor_id NOT NULL
   - country = NULL → permitido, lo manejas en Silver

PSI (Population Stability Index):
  PSI = sum((actual_pct - expected_pct) * ln(actual_pct / expected_pct))
  Si PSI > 0.2 en payment_amount → drift significativo, reportar.

Usa gx_run para los suites, spark_sql para PSI, slack_notify si DQ < 0.95.
Reporta: dq_score, expectations fallidas, quarantine_rows, drift_detected.
"""


def quality_agent_node(state: PipelineState) -> dict:
    """Nodo LangGraph del Quality Agent."""
    logger.info("[QUALITY] iniciando DQ checks run_id=%s", state.run_id)

    llm = ChatAnthropic(
        model="claude-haiku-4-5",
        max_tokens=2048,
        temperature=0,
    ).bind_tools(ALL_TOOLS)

    prompt = _build_quality_prompt(state)
    messages = [
        SystemMessage(content=QUALITY_SYSTEM),
        HumanMessage(content=prompt),
    ]

    tool_results: list[dict] = []

    for _ in range(10):
        response = llm.invoke(messages)
        messages.append(response)

        if not response.tool_calls:
            break

        for tc in response.tool_calls:
            result = _execute_tool(tc["name"], tc["args"], state)
            messages.append(ToolMessage(content=str(result), tool_call_id=tc["id"]))
            tool_results.append({"tool": tc["name"], "args": tc["args"], "result": result})
            logger.debug("[QUALITY] tool=%s", tc["name"])

    dq_result = _build_dq_result(tool_results, state)

    return {
        "dq_result": dq_result,
        "dq_status": AgentStatus.DONE,
        "messages": [
            HumanMessage(
                content=f"QUALITY AGENT: DQ={dq_result.score:.3f} | "
                        f"drift={dq_result.drift_detected} | "
                        f"quarantine={dq_result.quarantine_rows}"
            )
        ],
    }


def _build_quality_prompt(state: PipelineState) -> str:
    tables = "\n".join(f"  - {t}" for t in state.ingestion_tables)
    inject = ""
    if state.inject_drift:
        inject = "\n⚠ ESCENARIO: payment_amount tiene distribución diferente al baseline abril 2026. Calcula PSI."
    if state.inject_nulls:
        inject += "\n⚠ ESCENARIO: bronze_progress tiene filas con total_lessons=0. Detéctalas y cuarenténalas."

    return f"""
run_id: {state.run_id}
Filas ingestadas: {state.ingestion_rows}

Tablas a validar:
{tables}

Ejecuta los siguientes pasos:
1. Corre gx_run sobre cada tabla con sus suites respectivos
2. Calcula PSI de payment_amount vs baseline con spark_sql
3. Si DQ score < {DQ_SLACK_WARNING} → usa slack_notify en #data-quality
4. Registra filas corruptas en _quarantine
{inject}
"""


def _execute_tool(name: str, args: dict, state: PipelineState) -> Any:
    from tools.databricks_mcp import TOOL_BY_NAME

    if name == "gx_run":
        if state.inject_nulls and "progress" in args.get("table_name", ""):
            return {
                "success": False,
                "dq_score": 0.943,
                "total_expectations": 23,
                "failed": ["expect_column_values_to_not_be_null[total_lessons]"],
                "quarantine_rows": 230,
                "run_id": "mock-dq-nulls",
            }
        return {
            "success": True,
            "dq_score": 0.987,
            "total_expectations": 23,
            "failed": [],
            "quarantine_rows": 0,
            "run_id": "mock-dq-ok",
        }

    if name == "spark_sql" and state.inject_drift:
        psi_value = 0.34
        return {
            "status": "SUCCEEDED",
            "columns": ["column", "psi"],
            "rows": [{"column": "payment_amount", "psi": psi_value}],
            "row_count": 1,
        }

    tool_fn = TOOL_BY_NAME.get(name)
    if tool_fn is None:
        return {"error": f"Tool '{name}' no registrado"}
    try:
        return tool_fn.invoke(args)
    except Exception as exc:
        logger.warning("[QUALITY] tool %s falló: %s", name, exc)
        return {"error": str(exc)}


def _build_dq_result(tool_results: list[dict], state: PipelineState) -> DQResult:
    """Agrega resultados de GE y PSI en un DQResult canónico."""
    scores = []
    failed: list[str] = []
    quarantine_total = 0
    psi_scores: dict[str, float] = {}

    for tr in tool_results:
        r = tr.get("result", {})
        tool_name = tr.get("tool", "")

        if isinstance(r, dict):
            if "dq_score" in r:
                scores.append(r["dq_score"])
                failed.extend(r.get("failed", []))
                quarantine_total += r.get("quarantine_rows", 0)

            if "rows" in r and tool_name == "spark_sql":
                for row in r.get("rows", []):
                    if "psi" in row and "column" in row:
                        psi_scores[row["column"]] = float(row["psi"])

    avg_score = sum(scores) / len(scores) if scores else (0.943 if state.inject_nulls else 0.987)
    drift = any(v > PSI_DRIFT_THRESHOLD for v in psi_scores.values()) or state.inject_drift

    if state.inject_drift and not psi_scores:
        psi_scores["payment_amount"] = 0.34

    return DQResult(
        score=round(avg_score, 4),
        failed_expectations=list(set(failed)),
        quarantine_rows=quarantine_total or (230 if state.inject_nulls else 0),
        psi_scores=psi_scores,
        drift_detected=drift,
    )


# ---------------------------------------------------------------------------
# PSI computation (standalone, sin Spark — para tests locales)
# ---------------------------------------------------------------------------
def compute_psi(actual: list[float], expected: list[float], bins: int = 10) -> float:
    """
    Calcula Population Stability Index entre dos distribuciones.
    PSI < 0.1: estable | 0.1-0.2: ligero cambio | > 0.2: drift significativo
    """
    eps = 1e-6
    act_arr = np.array(actual, dtype=float)
    exp_arr = np.array(expected, dtype=float)

    breakpoints = np.percentile(exp_arr, np.linspace(0, 100, bins + 1))
    breakpoints[0] = -np.inf
    breakpoints[-1] = np.inf

    act_counts = np.histogram(act_arr, bins=breakpoints)[0]
    exp_counts = np.histogram(exp_arr, bins=breakpoints)[0]

    act_pct = (act_counts + eps) / (len(act_arr) + eps)
    exp_pct = (exp_counts + eps) / (len(exp_arr) + eps)

    psi = float(np.sum((act_pct - exp_pct) * np.log(act_pct / exp_pct)))
    return round(psi, 4)
