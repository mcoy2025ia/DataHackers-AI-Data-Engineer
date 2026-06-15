"""
pipelines/graph.py
Ensamblaje del grafo LangGraph — EduStream Agentic Pipeline.

Topología:
  START
    → supervisor (planifica y decide)
    → ingestion_agent
    → supervisor (re-evalúa)
    → quality_agent
    → supervisor (re-evalúa DQ score)
    → transformation_agent
    → supervisor (re-evalúa)
    → semantic_agent
    → supervisor (re-evalúa)
    → observability_agent
    → END

El supervisor siempre se ejecuta entre agentes para decidir si continuar,
reintentar, escalar a humano o ir directamente al siguiente paso.

Checkpointing: usa SqliteSaver en dev, MemorySaver en tests.
En producción → DatabricksSaver (DBFS).
"""

from __future__ import annotations

import logging
from typing import Any

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph

from agents.ingestion_agent      import ingestion_agent_node
from agents.observability_agent  import observability_agent_node
from agents.quality_agent        import quality_agent_node
from agents.semantic_agent       import semantic_agent_node
from agents.supervisor           import supervisor_node, supervisor_router
from agents.transformation_agent import transformation_agent_node
from config.state                import PipelineState

logger = logging.getLogger(__name__)


def build_pipeline_graph(checkpointer=None) -> CompiledStateGraph:
    """
    Construye y compila el grafo LangGraph del pipeline agentic de EduStream.

    Args:
        checkpointer: LangGraph checkpointer. None → MemorySaver (dev).
                      En producción usa DatabricksSaver o SqliteSaver.

    Returns:
        CompiledStateGraph listo para invocar.
    """
    builder = StateGraph(PipelineState)

    # --- Nodos ---
    builder.add_node("supervisor",            supervisor_node)
    builder.add_node("ingestion_agent",       ingestion_agent_node)
    builder.add_node("quality_agent",         quality_agent_node)
    builder.add_node("transformation_agent",  transformation_agent_node)
    builder.add_node("semantic_agent",        semantic_agent_node)
    builder.add_node("observability_agent",   observability_agent_node)

    # --- Edges: START → supervisor ---
    builder.add_edge(START, "supervisor")

    # --- Supervisor routing condicional ---
    # El supervisor decide el próximo nodo leyendo state.next_agent
    builder.add_conditional_edges(
        "supervisor",
        supervisor_router,
        {
            "ingestion_agent":      "ingestion_agent",
            "quality_agent":        "quality_agent",
            "transformation_agent": "transformation_agent",
            "semantic_agent":       "semantic_agent",
            "observability_agent":  "observability_agent",
            END:                    END,
        },
    )

    # --- Cada agente vuelve al supervisor para que re-evalúe ---
    for agent in [
        "ingestion_agent",
        "quality_agent",
        "transformation_agent",
        "semantic_agent",
        "observability_agent",
    ]:
        builder.add_edge(agent, "supervisor")

    cp = checkpointer or MemorySaver()
    graph = builder.compile(checkpointer=cp)

    logger.info("[GRAPH] Pipeline agentic compilado. Nodos: %s", list(builder.nodes))
    return graph


# ---------------------------------------------------------------------------
# Helpers de ejecución
# ---------------------------------------------------------------------------

def run_pipeline(
    run_id: str | None = None,
    env: str = "dev",
    inject_drift: bool = False,
    inject_nulls: bool = False,
    checkpointer=None,
) -> PipelineState:
    """
    Punto de entrada para correr el pipeline completo.

    Usage:
        from pipelines.graph import run_pipeline
        state = run_pipeline(env="prod")
        print(state.obs_result)
    """
    from datetime import datetime

    graph = build_pipeline_graph(checkpointer=checkpointer)

    initial_state = PipelineState(
        run_id=run_id or datetime.utcnow().strftime("%Y%m%d-%H%M%S"),
        env=env,
        inject_drift=inject_drift,
        inject_nulls=inject_nulls,
    )

    config = {"configurable": {"thread_id": initial_state.run_id}}

    logger.info(
        "[PIPELINE] Iniciando run_id=%s env=%s drift=%s nulls=%s",
        initial_state.run_id, env, inject_drift, inject_nulls,
    )

    final_state = graph.invoke(initial_state.model_dump(), config=config)

    logger.info(
        "[PIPELINE] Completado. SLO=%s | cost=$%.4f | DQ=%.3f",
        final_state.get("obs_result", {}).get("slo_ok", "N/A"),
        final_state.get("obs_result", {}).get("cost_usd", 0),
        final_state.get("dq_result", {}).get("score", 0) if final_state.get("dq_result") else 0,
    )

    return PipelineState(**final_state)


def get_graph_mermaid() -> str:
    """Devuelve la representación Mermaid del grafo para documentación."""
    graph = build_pipeline_graph()
    return graph.get_graph().draw_mermaid()


def resume_from_checkpoint(run_id: str, checkpointer) -> PipelineState:
    """
    Reanuda un pipeline que falló desde el último checkpoint guardado.
    Útil para recuperación sin reprocesar Bronze completo.

    Usage:
        from langgraph.checkpoint.sqlite import SqliteSaver
        saver = SqliteSaver.from_conn_string("checkpoints.db")
        state = resume_from_checkpoint("20260520-143022", saver)
    """
    graph = build_pipeline_graph(checkpointer=checkpointer)
    config = {"configurable": {"thread_id": run_id}}

    # LangGraph recupera el último estado válido desde el checkpointer
    checkpoint = checkpointer.get(config)
    if not checkpoint:
        raise ValueError(f"No hay checkpoint para run_id={run_id}")

    logger.info("[PIPELINE] Reanudando desde checkpoint run_id=%s", run_id)
    final_state = graph.invoke(None, config=config)
    return PipelineState(**final_state)
