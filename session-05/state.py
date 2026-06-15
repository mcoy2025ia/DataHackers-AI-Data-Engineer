"""
config/state.py
Pydantic models que definen el estado compartido del grafo LangGraph.
Cada agente lee y escribe sobre PipelineState — el orquestador lo
pasa entre nodos con checkpointing automático en SQLite / DBFS.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated, Any

from pydantic import BaseModel, Field
from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage


class AgentStatus(str, Enum):
    IDLE      = "idle"
    RUNNING   = "running"
    DONE      = "done"
    ERROR     = "error"
    QUARANTINE = "quarantine"


class DQResult(BaseModel):
    score: float = Field(ge=0.0, le=1.0)
    failed_expectations: list[str] = []
    quarantine_rows: int = 0
    psi_scores: dict[str, float] = {}   # Population Stability Index por columna
    drift_detected: bool = False


class TransformResult(BaseModel):
    silver_tables: list[str] = []
    gold_tables: list[str] = []
    scd2_changes: int = 0
    rows_written: dict[str, int] = {}


class SemanticResult(BaseModel):
    nl_question: str = ""
    generated_sql: str = ""
    result_rows: int = 0
    audit_passed: bool = False


class ObsResult(BaseModel):
    freshness_minutes: float = 0.0
    dq_score: float = 0.0
    cost_usd: float = 0.0
    tokens_total: int = 0
    slo_ok: bool = True
    mlflow_run_id: str = ""


class PipelineState(BaseModel):
    """
    Estado central del grafo LangGraph.
    `messages` usa el reducer add_messages de LangGraph para acumular
    el historial de conversación del supervisor sin sobrescribirlo.
    """
    messages: Annotated[list[BaseMessage], add_messages] = []

    run_id: str = Field(default_factory=lambda: datetime.utcnow().strftime("%Y%m%d-%H%M%S"))
    triggered_at: datetime = Field(default_factory=datetime.utcnow)
    env: str = "dev"                        # dev | staging | prod

    # --- flags de control ---
    inject_drift: bool = False              # para tests de chaos
    inject_nulls: bool = False

    # --- resultados por agente ---
    ingestion_status: AgentStatus = AgentStatus.IDLE
    ingestion_rows: int = 0
    ingestion_tables: list[str] = []

    dq_result: DQResult | None = None
    dq_status: AgentStatus = AgentStatus.IDLE

    transform_result: TransformResult | None = None
    transform_status: AgentStatus = AgentStatus.IDLE

    semantic_result: SemanticResult | None = None
    semantic_status: AgentStatus = AgentStatus.IDLE

    obs_result: ObsResult | None = None
    obs_status: AgentStatus = AgentStatus.IDLE

    # --- decisiones del supervisor ---
    supervisor_decisions: list[str] = []
    next_agent: str = "ingestion_agent"     # primer nodo a ejecutar
    pipeline_complete: bool = False
    error_message: str | None = None

    class Config:
        arbitrary_types_allowed = True
