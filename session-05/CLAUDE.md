# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## Project Overview

**EduStream Agentic Data Platform** — An autonomous data pipeline using LangGraph orchestration, Claude for reasoning, and Databricks for execution. Six specialized agents (Ingestion, Quality, Transformation, Semantic, Observability) are routed by a Supervisor agent that plans decisions, handles checkpointing, and recovers from failures.

**Key Distinction**: Unlike static ETL, this is an *agentic* medallion architecture where the Supervisor continuously re-evaluates state and decides next steps — schema drift triggers auto-remediation, data quality issues branch to triage agents, and the pipeline can be resumed from checkpoints.

---

## Tech Stack & Critical Dependencies

| Layer | Tech | Purpose |
|-------|------|---------|
| **Orchestration** | LangGraph 0.2 | State machine + checkpointing for agent coordination |
| **LLM Routing** | Claude Sonnet/Haiku 4.5 | Supervisor reasoning; deterministic tasks use Haiku |
| **Lakehouse** | Databricks + Delta Lake | Data layer with Unity Catalog |
| **State** | Pydantic models in `state.py` | Immutable, validated pipeline state shared across agents |
| **Checkpointing** | SqliteSaver (dev) / MemorySaver (test) | Resume from failure; prod uses DatabricksSaver |
| **Data Quality** | Great Expectations 1.2 | GE suites + PSI (Population Stability Index) |
| **Observability** | Logfire + OpenTelemetry + MLflow | Cost tracking, SLO monitoring, experiment logging |

---

## Commands

```bash
# Install dependencies (Poetry required)
poetry install

# Run tests (no Databricks required, mocked)
pytest tests/ -v
pytest tests/ -v -k "test_quality"  # single agent

# Run pipeline normally
python main.py

# Chaos testing modes
python main.py --drift          # inject distribution drift
python main.py --nulls          # inject data corruption
python main.py --env prod       # production environment

# Resume from checkpoint (if a run failed mid-pipeline)
python main.py --resume 20260520-143022
```

---

## Architecture: Agent Graph & State Flow

### Pipeline Topology

```
START
  ↓
[Supervisor] ← routes based on state.next_agent
  ↓ ↓ ↓ ↓ ↓
[Ingestion] → [Supervisor] → [Quality] → [Supervisor] → [Transformation] → ...
                                                          ↓
                                                    [Supervisor] → [Semantic] → [Supervisor] → [Observability]
                                                                                                    ↓
                                                                                                  END
```

**Key pattern**: After *every* agent, control flows back to `supervisor_node`. The supervisor:
1. Reads `state.next_agent` (what the previous agent left behind)
2. Evaluates the pipeline state (rows processed, DQ scores, SLO status)
3. Decides via LLM: "Should we continue, retry, escalate, or skip to observability?"
4. Sets `state.next_agent` for the conditional edge to route to

### State Management: `config/state.py`

`PipelineState` (Pydantic) is the **single source of truth** passed by LangGraph between nodes. Never create new state objects — always mutate via `state.model_copy(update={...})`.

**Message accumulation**: The `messages` field uses LangGraph's `add_messages` reducer, so each agent's output appends to the conversation history without overwriting. This enables the Supervisor to reason over the full execution transcript.

Critical fields:
- `next_agent`: String set by agents/supervisor; routes the conditional edge
- `pipeline_complete`: Boolean; Supervisor sets to True when done
- `ingestion_rows`, `dq_result`, `transform_result`: Results from each agent stage
- `supervisor_decisions`: List[str] tracking every decision made (for audit trail)

---

## Key Files & Responsibilities

### Entry Point & CLI
- **`main.py`**: Argument parsing, exception handling, final summary printing. Uses `argparse` for `--env`, `--drift`, `--nulls`, `--resume`.

### Configuration
- **`state.py`**: Pydantic models — `PipelineState`, `DQResult`, `TransformResult`, `SemanticResult`, `ObsResult`. Also defines `AgentStatus` enum (IDLE, RUNNING, DONE, ERROR, QUARANTINE).

### Agents (in `agents/`)
Each agent is a stateless function `def agent_node(state: PipelineState) -> PipelineState`. **All I/O is mocked in tests** (no real Databricks calls).

- **`supervisor.py`**: `supervisor_node()` calls the LLM with current state context and decisions it. `supervisor_router()` implements the conditional edge logic. Uses Sonnet for complex reasoning.
- **`ingestion_agent.py`**: Loads CSVs → Bronze tables, detects schema drift. Uses Haiku (deterministic task).
- **`quality_agent.py`**: Runs GE expectations, computes PSI per column, flags drift. Quarantines rows if needed.
- **`transformation_agent.py`**: dbt + PySpark to build Silver (SCD2) and Gold (star schema). Sonnet reasons about schema/lineage.
- **`semantic_agent.py`**: Text2SQL — takes natural language questions, generates SQL against the catalog, audits for injection.
- **`observability_agent.py`**: Computes freshness, SLO compliance, LLM cost, logs to MLflow. Haiku (metrics-only).

### Tools
- **`tools/databricks_mcp.py`**: MCP server defining 5 tools: `spark_sql`, `catalog_search`, `gx_run`, `mlflow_log`, `slack_notify`. These are invoked as tool calls by agents.

### Pipeline Assembly
- **`graph.py`**: `build_pipeline_graph()` constructs the StateGraph, adds nodes and conditional edges. Returns a CompiledStateGraph. Also includes `run_pipeline()` and `resume_from_checkpoint()` helpers.

### Testing
- **`test_pipeline.py`**: Pytest suite with fixtures for `PipelineState` snapshots. Tests state mutations, agent outputs (mocked), and PSI computation. Run with `pytest tests/ -v`.

### Contracts
- **`contracts/data_contracts.json`**: JSON Schema defining expected fields per source, GE expectation suite references, Silver transformation rules, SLO thresholds. Versioned in Git; schema drift triggers alerts.

---

## Design Patterns & Conventions

### State Immutability in Agents
Agents never mutate `state` in-place. Return a new state:
```python
return state.model_copy(update={
    "ingestion_status": AgentStatus.DONE,
    "ingestion_rows": 50000,
})
```

### LLM Model Selection
- **Supervisor**: `claude-sonnet-4-5` — requires complex reasoning (routing, recovery decisions)
- **Deterministic agents** (Ingestion, Quality, Observability): `claude-haiku-4-5` — ~70% cost savings
- **Complex reasoning** (Transformation with SCD2, Semantic with catalog context): `claude-sonnet-4-5`

### Supervisor Decision Logging
After each supervisor call, append the decision to `state.supervisor_decisions`:
```python
decision_str = f"Route to {next_agent}: DQ score {dq_score:.3f} → {'PASS' if dq_score >= 0.95 else 'REVIEW'}"
state.supervisor_decisions.append(decision_str)
```
This creates an audit trail visible in the final summary.

### Checkpointing Strategy
- **Dev**: SQLite (`checkpoints_dev.db`) for local testing and resume
- **Test**: MemorySaver (ephemeral, no disk I/O)
- **Prod**: DatabricksSaver (DBFS) for fault tolerance

Resume works by passing `run_id` to `resume_from_checkpoint()`, which loads the last state snapshot and replays the graph from that point.

### Testing Without Databricks
All Databricks API calls in tests are mocked via `unittest.mock.patch`. Tests run on the local CPU, so PRs can be validated in CI without cloud credentials. The `@pytest.fixture` pattern provides reusable state snapshots.

---

## Common Workflows

### Adding a New Agent
1. Create `agents/new_agent.py` with `def new_agent_node(state: PipelineState) -> PipelineState`
2. Add imports and node definition in `graph.py`
3. Add edge: `builder.add_edge("new_agent", "supervisor")` (always returns to supervisor)
4. Add conditional route in `supervisor_router()` for the new agent path
5. Update tests: add fixture for state snapshot after the new agent; mock its I/O

### Modifying Pipeline Logic
- **State changes**: Edit `state.py` (Pydantic model); breaking changes require migration logic
- **Routing logic**: Edit `supervisor_node()` to change decision criteria; test with `--drift` or `--nulls`
- **Agent behavior**: Edit the agent node function; mocks in tests absorb the change

### Debugging a Failed Run
1. Check the final summary in logs — supervisor_decisions list shows the decision path
2. If it failed mid-pipeline, use `--resume <run_id>` to pick up from the last checkpoint
3. Run `pytest tests/ -v -k "test_<agent_name>"` to isolate the agent logic

---

## Environment Variables Required

```
ANTHROPIC_API_KEY       # Required for Claude API calls
DATABRICKS_HOST         # https://your-workspace.azuredatabricks.net
DATABRICKS_TOKEN        # dapi...
SLACK_WEBHOOK_URL       # Optional; for alerts
```

Tests do not need these (all calls are mocked).

---

## Important Notes

- **Pydantic v2**: The codebase uses Pydantic v2 syntax (`model_copy`, `Field`). Never use v1 patterns.
- **LangGraph 0.2**: Checkpointing and conditional routing are stable; MemorySaver and SqliteSaver work without Databricks.
- **No hardcoded Databricks in tests**: All Databricks SDK calls are patched, so tests run instantly on local CPU.
- **Git-versioned contracts**: `data_contracts.json` is the source of truth for schema; drift detection compares live data against this.
- **Supervisor is the bottleneck**: The Supervisor runs LLM inference on every state transition. In production, consider batching non-critical decisions or adding a "fast-path" router for deterministic transitions.
