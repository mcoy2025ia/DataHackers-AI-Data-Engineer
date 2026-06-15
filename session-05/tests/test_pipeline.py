"""
tests/test_pipeline.py
Test suite completo del pipeline agentic de EduStream.
Corre sin Databricks real — usa mocks para todas las tool calls.

Ejecutar:
    pytest tests/ -v --tb=short
    pytest tests/ -v -k "test_quality"  # un agente específico
"""

from __future__ import annotations

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from config.state import AgentStatus, DQResult, PipelineState, TransformResult, SemanticResult
from agents.quality_agent import compute_psi
from pipelines.graph import build_pipeline_graph


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def base_state() -> PipelineState:
    return PipelineState(
        run_id="test-20260520",
        env="test",
        inject_drift=False,
        inject_nulls=False,
    )


@pytest.fixture
def state_after_ingestion(base_state) -> PipelineState:
    return base_state.model_copy(update={
        "ingestion_status": AgentStatus.DONE,
        "ingestion_rows": 47820,
        "ingestion_tables": [
            "edustream.bronze.enrollments",
            "edustream.bronze.courses",
            "edustream.bronze.progress",
            "edustream.bronze.instructors",
        ],
    })


@pytest.fixture
def state_after_quality(state_after_ingestion) -> PipelineState:
    return state_after_ingestion.model_copy(update={
        "dq_status": AgentStatus.DONE,
        "dq_result": DQResult(
            score=0.987,
            failed_expectations=[],
            quarantine_rows=0,
            psi_scores={"payment_amount": 0.05},
            drift_detected=False,
        ),
    })


# ---------------------------------------------------------------------------
# Tests: PipelineState
# ---------------------------------------------------------------------------

class TestPipelineState:
    def test_default_values(self, base_state):
        assert base_state.ingestion_status == AgentStatus.IDLE
        assert base_state.pipeline_complete is False
        assert base_state.next_agent == "ingestion_agent"

    def test_run_id_format(self, base_state):
        assert base_state.run_id == "test-20260520"

    def test_inject_flags_default_false(self, base_state):
        assert base_state.inject_drift is False
        assert base_state.inject_nulls is False

    def test_state_immutable_copy(self, base_state):
        updated = base_state.model_copy(update={"ingestion_rows": 1000})
        assert base_state.ingestion_rows == 0
        assert updated.ingestion_rows == 1000


# ---------------------------------------------------------------------------
# Tests: Quality Agent — compute_psi
# ---------------------------------------------------------------------------

class TestPSIComputation:
    def test_identical_distributions_near_zero(self):
        data = [float(i) for i in range(1000)]
        psi = compute_psi(data, data)
        assert psi < 0.05, f"PSI de distribuciones idénticas debería ser ~0, got {psi}"

    def test_different_distributions_high_psi(self):
        expected = [float(i) for i in range(1000)]
        actual   = [float(i) * 3 + 500 for i in range(1000)]
        psi = compute_psi(actual, expected)
        assert psi > 0.2, f"PSI de distribuciones muy diferentes debería ser >0.2, got {psi}"

    def test_moderate_drift_between_thresholds(self):
        import random
        random.seed(42)
        expected = [random.gauss(100, 20) for _ in range(1000)]
        actual   = [random.gauss(115, 22) for _ in range(1000)]
        psi = compute_psi(actual, expected)
        assert 0.05 <= psi <= 0.5, f"PSI de drift moderado esperado entre 0.05-0.5, got {psi}"


# ---------------------------------------------------------------------------
# Tests: Ingestion Agent node (mock tools)
# ---------------------------------------------------------------------------

class TestIngestionAgent:
    @patch("agents.ingestion_agent._execute_tool")
    @patch("agents.ingestion_agent.ChatAnthropic")
    def test_normal_ingestion(self, mock_llm_cls, mock_tool, state_after_ingestion):
        from agents.ingestion_agent import ingestion_agent_node

        mock_llm = MagicMock()
        mock_llm.bind_tools.return_value = mock_llm
        mock_llm.invoke.return_value = MagicMock(
            tool_calls=[],
            content="Ingesta completada: 47820 filas en 4 tablas Bronze.",
        )
        mock_llm_cls.return_value = mock_llm
        mock_tool.return_value = {"affected_rows": 47820}

        initial = PipelineState(run_id="test-ing", env="test")
        result = ingestion_agent_node(initial)

        assert result["ingestion_status"] == AgentStatus.DONE
        assert result["ingestion_rows"] > 0
        assert len(result["ingestion_tables"]) == 4

    @patch("agents.ingestion_agent._execute_tool")
    @patch("agents.ingestion_agent.ChatAnthropic")
    def test_ingestion_with_nulls_injection(self, mock_llm_cls, mock_tool, base_state):
        from agents.ingestion_agent import ingestion_agent_node

        mock_llm = MagicMock()
        mock_llm.bind_tools.return_value = mock_llm
        mock_llm.invoke.return_value = MagicMock(tool_calls=[], content="Listo.")
        mock_llm_cls.return_value = mock_llm
        mock_tool.return_value = {"affected_rows": 230, "rows": 230}

        state = base_state.model_copy(update={"inject_nulls": True})
        result = ingestion_agent_node(state)

        assert result["ingestion_status"] == AgentStatus.DONE


# ---------------------------------------------------------------------------
# Tests: Quality Agent — DQResult building
# ---------------------------------------------------------------------------

class TestQualityAgent:
    def test_dq_result_with_no_injections(self, state_after_ingestion):
        from agents.quality_agent import _build_dq_result
        tool_results = [
            {"tool": "gx_run", "result": {"dq_score": 0.987, "failed": [], "quarantine_rows": 0}}
        ]
        dq = _build_dq_result(tool_results, state_after_ingestion)
        assert dq.score == 0.987
        assert dq.quarantine_rows == 0
        assert dq.drift_detected is False

    def test_dq_result_with_null_injection(self, base_state):
        from agents.quality_agent import _build_dq_result
        state = base_state.model_copy(update={"inject_nulls": True})
        tool_results = [
            {"tool": "gx_run", "result": {
                "dq_score": 0.943,
                "failed": ["expect_column_values_to_not_be_null[total_lessons]"],
                "quarantine_rows": 230,
            }}
        ]
        dq = _build_dq_result(tool_results, state)
        assert dq.score == 0.943
        assert dq.quarantine_rows == 230
        assert "expect_column_values_to_not_be_null[total_lessons]" in dq.failed_expectations

    def test_drift_detected_with_high_psi(self, base_state):
        from agents.quality_agent import _build_dq_result
        state = base_state.model_copy(update={"inject_drift": True})
        tool_results = [
            {"tool": "spark_sql", "result": {
                "rows": [{"column": "payment_amount", "psi": 0.34}]
            }}
        ]
        dq = _build_dq_result(tool_results, state)
        assert dq.drift_detected is True
        assert dq.psi_scores["payment_amount"] == 0.34


# ---------------------------------------------------------------------------
# Tests: Semantic Agent — SQL audit
# ---------------------------------------------------------------------------

class TestSemanticAgent:
    def test_audit_passes_gold_only_sql(self):
        from agents.semantic_agent import _audit_sql
        sql = "SELECT category, SUM(revenue_usd) FROM edustream.gold.agg_kpi_monthly GROUP BY 1"
        assert _audit_sql(sql) is True

    def test_audit_fails_bronze_access(self):
        from agents.semantic_agent import _audit_sql
        sql = "SELECT * FROM edustream.bronze.enrollments"
        assert _audit_sql(sql) is False

    def test_audit_fails_silver_access(self):
        from agents.semantic_agent import _audit_sql
        sql = "SELECT user_id FROM edustream.silver.progress WHERE completion_flag"
        assert _audit_sql(sql) is False

    def test_audit_fails_select_star(self):
        from agents.semantic_agent import _audit_sql
        sql = "SELECT * FROM edustream.gold.dim_course"
        assert _audit_sql(sql) is False

    def test_extract_sql_from_markdown(self):
        from agents.semantic_agent import _extract_sql_from_text
        text = """Aquí está el SQL:
```sql
SELECT category, SUM(revenue_usd)
FROM gold.agg_kpi_monthly
GROUP BY category
```
Esto responde tu pregunta."""
        sql = _extract_sql_from_text(text)
        assert "SELECT" in sql.upper()
        assert "gold.agg_kpi_monthly" in sql


# ---------------------------------------------------------------------------
# Tests: Full pipeline graph (integration, con MemorySaver)
# ---------------------------------------------------------------------------

class TestPipelineGraph:
    def test_graph_builds_without_error(self):
        graph = build_pipeline_graph()
        assert graph is not None

    def test_graph_has_all_nodes(self):
        graph = build_pipeline_graph()
        node_names = set(graph.get_graph().nodes.keys())
        expected = {
            "supervisor", "ingestion_agent", "quality_agent",
            "transformation_agent", "semantic_agent", "observability_agent",
        }
        assert expected.issubset(node_names), f"Nodos faltantes: {expected - node_names}"

    @patch("agents.ingestion_agent.ChatAnthropic")
    @patch("agents.quality_agent.ChatAnthropic")
    @patch("agents.transformation_agent.ChatAnthropic")
    @patch("agents.semantic_agent.ChatAnthropic")
    @patch("agents.observability_agent.ChatAnthropic")
    @patch("agents.supervisor.ChatAnthropic")
    def test_full_pipeline_happy_path(
        self, mock_sup, mock_obs, mock_sem, mock_tr, mock_qa, mock_ing
    ):
        """
        Test de integración del pipeline completo.
        Mockea todos los LLMs — verifica el flujo de estado, no el contenido LLM.
        """
        def make_mock_llm(tool_calls_seq=None):
            llm = MagicMock()
            llm.bind_tools.return_value = llm
            llm.with_structured_output.return_value = llm
            responses = []
            if tool_calls_seq:
                for tc in tool_calls_seq:
                    responses.append(MagicMock(tool_calls=tc, content=""))
            responses.append(MagicMock(tool_calls=[], content="Completado."))
            llm.invoke.side_effect = responses
            return llm

        from agents.supervisor import SupervisorDecision
        from langgraph.graph import END as LEND

        sup_llm = MagicMock()
        sup_llm.bind_tools.return_value = sup_llm

        sequence = [
            "ingestion_agent", "quality_agent", "transformation_agent",
            "semantic_agent", "observability_agent", LEND,
        ]
        call_count = [0]
        def sup_invoke(msgs):
            idx = min(call_count[0], len(sequence) - 1)
            call_count[0] += 1
            return SupervisorDecision(next=sequence[idx], rationale="test routing")

        sup_llm.with_structured_output.return_value = MagicMock(invoke=sup_invoke)
        mock_sup.return_value = sup_llm

        for mock_agent in [mock_ing, mock_qa, mock_tr, mock_sem, mock_obs]:
            mock_agent.return_value = make_mock_llm()

        from pipelines.graph import run_pipeline
        final = run_pipeline(run_id="test-full-happy", env="test")

        assert final.ingestion_status == AgentStatus.DONE
        assert final.pipeline_complete is True
