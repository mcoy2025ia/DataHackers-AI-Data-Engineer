"""
agents/supervisor.py
Supervisor Agent — cerebro del grafo LangGraph.
Usa Claude Sonnet 4.5 como router + planner. Recibe el estado actual
del pipeline y decide qué agente ejecutar a continuación.

Patrón: ReAct con structured output (next_agent + decision_rationale).
El supervisor NO ejecuta nada — solo razona y delega.
"""

from __future__ import annotations

import logging
from typing import Literal

from anthropic import Anthropic
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END
from pydantic import BaseModel, Field

from config.state import AgentStatus, PipelineState

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Modelos que el supervisor puede invocar
# ---------------------------------------------------------------------------
AGENT_REGISTRY = Literal[
    "ingestion_agent",
    "quality_agent",
    "transformation_agent",
    "semantic_agent",
    "observability_agent",
    END,
]


class SupervisorDecision(BaseModel):
    """Structured output del supervisor — LangGraph lee `next` para el routing."""
    next: AGENT_REGISTRY = Field(
        description="Próximo agente a ejecutar, o '__end__' si el pipeline completó."
    )
    rationale: str = Field(
        description="Justificación de 1-2 oraciones de por qué este agente ahora."
    )
    escalate_to_human: bool = Field(
        default=False,
        description="True si el supervisor considera que un humano debe intervenir."
    )


SUPERVISOR_SYSTEM = """
Eres el Supervisor del pipeline de datos agentic de EduStream.
Tu única responsabilidad es orquestar los agentes especializados en el orden correcto.

ORDEN CANÓNICO DEL PIPELINE:
1. ingestion_agent  — carga CSV/streams a Bronze
2. quality_agent    — valida DQ y detecta drift
3. transformation_agent — Bronze→Silver→Gold con dbt + PySpark
4. semantic_agent   — genera SQL desde preguntas de negocio
5. observability_agent  — registra SLOs y cierra con MLflow
6. __end__          — pipeline completo

REGLAS DE ROUTING:
- Si un agente tiene status ERROR → considera si reintentarlo o escalar a humano
- Si DQ score < 0.90 → escala a humano antes de transformation_agent
- Si DQ score entre 0.90-0.95 → continúa pero registra decisión y notifica Slack
- Si drift_detected=True → ejecuta quality_agent de nuevo con PSI extendido
- Nunca saltes un agente sin justificación explícita
- El pipeline es idempotente — si un agente ya tiene status DONE, avanza al siguiente

Responde SOLO con el JSON estructurado: {next, rationale, escalate_to_human}
"""


def supervisor_node(state: PipelineState) -> dict:
    """
    Nodo LangGraph del Supervisor.
    Lee el estado completo, llama a Claude Sonnet 4.5 con structured output,
    y retorna el próximo agente a ejecutar + decisión registrada.
    """
    llm = ChatAnthropic(
        model="claude-sonnet-4-5",
        max_tokens=512,
        temperature=0,
    ).with_structured_output(SupervisorDecision)

    context = _build_context(state)

    messages = [
        SystemMessage(content=SUPERVISOR_SYSTEM),
        HumanMessage(content=context),
    ]

    logger.info("[SUPERVISOR] evaluando estado del pipeline run_id=%s", state.run_id)

    decision: SupervisorDecision = llm.invoke(messages)

    logger.info("[SUPERVISOR] → %s | %s", decision.next, decision.rationale)

    updated_decisions = state.supervisor_decisions + [
        f"[{decision.next}] {decision.rationale}"
    ]

    return {
        "next_agent": decision.next,
        "supervisor_decisions": updated_decisions,
        "pipeline_complete": decision.next == END,
        "messages": [HumanMessage(content=f"SUPERVISOR → {decision.next}: {decision.rationale}")],
    }


def _build_context(state: PipelineState) -> str:
    """Serializa el estado relevante del pipeline para el supervisor."""
    lines = [
        f"run_id: {state.run_id}",
        f"env: {state.env}",
        "",
        "=== ESTADO DE AGENTES ===",
        f"ingestion:       {state.ingestion_status.value} | filas={state.ingestion_rows}",
        f"quality:         {state.dq_status.value} | dq={state.dq_result.score if state.dq_result else 'N/A'} | drift={state.dq_result.drift_detected if state.dq_result else False}",
        f"transformation:  {state.transform_status.value}",
        f"semantic:        {state.semantic_status.value}",
        f"observability:   {state.obs_status.value}",
        "",
        "=== FLAGS DE INYECCIÓN (tests) ===",
        f"inject_drift: {state.inject_drift}",
        f"inject_nulls: {state.inject_nulls}",
        "",
        "=== DECISIONES PREVIAS ===",
    ]
    for d in state.supervisor_decisions[-5:]:
        lines.append(f"  - {d}")

    if state.error_message:
        lines.append(f"\n!!! ERROR ACTIVO: {state.error_message}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Router function para LangGraph — mapea next_agent al nodo correcto
# ---------------------------------------------------------------------------
def supervisor_router(state: PipelineState) -> str:
    """
    Función de routing de LangGraph.
    Llama al supervisor_node y retorna el nombre del próximo nodo.
    """
    return state.next_agent if state.next_agent else END
