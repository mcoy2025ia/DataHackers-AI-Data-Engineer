# EduStream Agentic Data Platform

**Arquitectura Medallion + Multi-Agent Orchestration · LangGraph + MCP + Databricks**

> Entregable S05 — DataHackers Academy AI Data Engineer Bootcamp  
> Autor: Manuel Coy · MCOY AI + Data Strategy

---

## Qué construimos

Un pipeline de datos **agentic** que va más allá del medallion estándar:
en lugar de ETL estático, seis agentes especializados orquestados por un Supervisor
(Claude Sonnet 4.5 via LangGraph) que razona sobre el estado del pipeline y decide
autónomamente qué ejecutar, cuándo escalar a humanos y cómo recuperarse de fallos.

```
START → Supervisor → Ingestion → Supervisor → Quality → Supervisor
      → Transformation → Supervisor → Semantic → Supervisor → Observability → END
```

---

## Stack técnico

| Capa | Tecnología |
|------|-----------|
| Orquestación | LangGraph 0.2 + Claude Sonnet/Haiku 4.5 |
| Lakehouse | Databricks + Delta Lake + Unity Catalog |
| Transformación | dbt-databricks + PySpark |
| Data Quality | Great Expectations 1.2 |
| MCP Tools | FastMCP (spark_sql, catalog_search, gx_run, mlflow_log, slack_notify) |
| Observabilidad | Logfire + OpenTelemetry + MLflow |
| Testing | pytest + MemorySaver (sin Databricks real) |

---

## Estructura del proyecto

```
edustream_agents/
├── main.py                        # CLI entrypoint
├── pyproject.toml
├── config/
│   └── state.py                   # PipelineState (Pydantic + LangGraph)
├── agents/
│   ├── supervisor.py              # Router + planner (Sonnet 4.5)
│   ├── ingestion_agent.py         # Bronze · Auto Loader · schema drift
│   ├── quality_agent.py           # GE · PSI · quarantine
│   ├── transformation_agent.py    # Silver SCD2 · Gold star schema
│   ├── semantic_agent.py          # Text2SQL · SQL audit
│   └── observability_agent.py     # SLO · MLflow · Logfire
├── tools/
│   └── databricks_mcp.py          # MCP Tools Layer (5 tools)
├── pipelines/
│   └── graph.py                   # StateGraph assembly + run helpers
├── contracts/
│   └── data_contracts.json        # JSON Schema + GE expectations
└── tests/
    └── test_pipeline.py           # 18 tests · pytest
```

---

## Instalación

```bash
# 1. Clonar y entrar al directorio
cd edustream_agents

# 2. Instalar dependencias con Poetry
pip install poetry
poetry install

# 3. Variables de entorno
export ANTHROPIC_API_KEY="sk-ant-..."
export DATABRICKS_HOST="https://your-workspace.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."
export SLACK_WEBHOOK_URL="https://hooks.slack.com/..."  # opcional
```

---

## Uso

```bash
# Pipeline normal (dev)
python main.py

# Inyectar drift para probar el Quality Agent
python main.py --drift

# Inyectar datos corruptos
python main.py --nulls

# Producción
python main.py --env prod

# Reanudar desde checkpoint (si un run falló)
python main.py --resume 20260520-143022

# Correr tests (sin Databricks)
pytest tests/ -v
```

---

## Por qué agentic vs ETL estático

| Problema | ETL estático | Este pipeline |
|----------|-------------|---------------|
| Fallo a mitad | Reprocesa todo desde 0 | Checkpointing: retoma desde el último agente |
| Drift detectado | Job falla con error | Quality Agent abre PR en dbt automáticamente |
| Nuevo campo en CSV | Pipeline breaks | Ingestion Agent detecta schema drift y notifica |
| CFO pregunta métricas | Ticket al equipo de datos | Semantic Agent responde en lenguaje natural |
| Costo LLM alto | N/A | Haiku para tasks determinísticas, Sonnet solo donde necesario |

---

## Agentes y modelos

| Agente | Modelo | Razón |
|--------|--------|-------|
| Supervisor | claude-sonnet-4-5 | Necesita razonamiento complejo para routing |
| Ingestion | claude-haiku-4-5 | Tarea determinística, costo bajo |
| Quality | claude-haiku-4-5 | Reglas explícitas, no necesita razonamiento profundo |
| Transformation | claude-sonnet-4-5 | SCD2 y star schema requieren contexto amplio |
| Semantic | claude-sonnet-4-5 | Text2SQL con razonamiento sobre el catálogo |
| Observability | claude-haiku-4-5 | Métricas numéricas, sin razonamiento |

**Ahorro estimado**: usar Haiku donde es suficiente reduce el costo ~70% vs Sonnet para todo.

---

## Data Contracts

Los contratos en `contracts/data_contracts.json` definen:
- Schema esperado por fuente (campos, tipos, nullabilidad)
- Great Expectations suites vinculadas
- Reglas de Silver (filtros, imputaciones, SCD2)
- SLO definitions y umbrales PSI para drift

Están versionados en Git. Un cambio de schema en el CSV sin actualizar el contrato
activa una alerta del Ingestion Agent.

---

## SLOs monitoreados

| SLO | Target | Consecuencia si falla |
|-----|--------|-----------------------|
| Freshness | < 60 min | Slack alert #cdao-alerts |
| DQ score | ≥ 0.95 | Slack alert + supervisor decide si continuar |
| Costo por corrida | < $0.50 USD | Log + revisión manual |
| Latencia pipeline | < 30 min | Slack alert |
