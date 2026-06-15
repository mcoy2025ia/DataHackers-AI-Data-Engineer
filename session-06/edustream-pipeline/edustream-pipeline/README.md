# EduStream — Pipeline de datos enterprise

Pipeline medallion sobre la plataforma educativa EduStream, construido en
Databricks. Implementa upserts idempotentes, una dimensión SCD Type 2, un
framework de data quality con gating, y una suite de tests ejecutable fuera de
Databricks.

Entregable de la Sesión 06 — AI Data Engineer Bootcamp (DataHackers Academy).

---

## Qué resuelve

El negocio necesita responder de forma confiable: ¿qué cursos se completan más?,
¿cuánto revenue genera cada categoría?, ¿cómo cambia el precio de un curso en el
tiempo? El reto no es calcular eso una vez — es un pipeline que lo calcule
**correctamente en cada corrida**, tolere datos sucios y sea re-ejecutable sin
efectos secundarios.

Decisiones de diseño y trade-offs: ver [`docs/DESIGN.md`](docs/DESIGN.md).

---

## Arquitectura

```
landing (Volume CSV)
   │  Auto Loader · schema explícito · rescuedDataColumn
   ▼
BRONZE   espejo inmutable del raw + linaje
   │  limpieza · dedup · DQ gating
   ▼
SILVER   datos validados ──▶ DIM_COURSES (SCD2)
   │  agregación
   ▼
GOLD     métricas listas para BI
```

---

## Stack

| Componente | Tecnología |
|------------|-----------|
| Plataforma | Databricks Free Edition · Serverless · Unity Catalog |
| Almacenamiento | Delta Lake |
| Ingestión | Auto Loader (`cloudFiles`) |
| Orquestación declarativa | Delta Live Tables |
| Patrones | Medallion · MERGE idempotente · SCD Type 2 · DQ gating |
| Optimización | OPTIMIZE · ZORDER · VACUUM · Auto Compaction |
| Lenguaje | PySpark · SQL |
| Testing | pytest sobre Spark local |

---

## Qué lo diferencia de un pipeline básico

| Aspecto | Enfoque básico | Este pipeline |
|---------|----------------|---------------|
| Carga de silver | `overwrite` (no incremental) | `MERGE INTO` idempotente |
| Schema | `inferSchema` (frágil) | `StructType` explícito = contrato |
| Cursos | Tabla de estado actual | Dimensión SCD Type 2 historizada |
| Calidad | `assert` dispersos | Framework `run_dq_check` + tabla `_dq_audit` |
| Schema drift | Rompe la carga | `rescuedDataColumn` lo captura |
| Tests | Ninguno | 10 tests de lógica, ejecutables sin Databricks |
| Decisiones | Implícitas | Documentadas como ADRs |

---

## Estructura del repositorio

```
edustream-pipeline/
├── README.md
├── docs/
│   └── DESIGN.md                      documento de diseño + ADRs
├── notebooks/
│   ├── S06_EduStream_Pipeline.py      pipeline batch (bronze→silver→dim→gold)
│   └── S06_EduStream_DLT.py           pipeline streaming declarativo (DLT)
├── data/
│   └── generate_edustream_data.py     generador de datos sintéticos (seed=42)
├── tests/
│   └── test_pipeline_logic.py         suite pytest — 10 tests
└── screenshots/
    ├── 01_catalog_explorer.png
    ├── 02_dlt_pipeline_graph.png
    ├── 03_dlt_data_quality.png
    └── 04_dq_audit_table.png
```

---

## Cómo reproducirlo

### 1. Generar el dataset

El repo no incluye CSVs — los genera un script determinista. Inyecta
imperfecciones controladas (nulos, negativos, duplicados) para que el pipeline
demuestre que las maneja.

```bash
cd data
python generate_edustream_data.py --out . --enrollments 5000
```

### 2. Correr los tests (opcional, recomendado)

Valida la lógica de transformación sin necesidad de Databricks.

```bash
pip install pyspark pytest
python -m pytest tests/ -v
```

### 3. Preparar Databricks

1. Cuenta en [Databricks Free Edition](https://signup.databricks.com) — sin tarjeta.
2. Catalog → crear schema `edustream` → dentro, crear Volume `landing`.
3. Subir los 4 CSV generados al Volume `landing`.

### 4. Pipeline batch

Importar `notebooks/S06_EduStream_Pipeline.py`, conectar a Serverless, ejecutar
las celdas en orden. Construye bronze, silver, `dim_courses` (SCD2), gold,
demuestra time travel y aplica las optimizaciones.

### 5. Pipeline DLT

`Jobs & Pipelines` → `Create` → `ETL Pipeline`. Source code: el notebook
`S06_EduStream_DLT.py`. Destination: catalog `workspace`, schema `edustream`.
Compute Serverless. `Start` y observar el DAG.

---

## Reflexiones técnicas

Las cuatro preguntas del entregable, respondidas. El detalle completo de cada
decisión está en [`docs/DESIGN.md`](docs/DESIGN.md).

**1 · Managed vs External table.** Managed: Unity Catalog controla metadatos y
archivos; `DROP` borra ambos. External: tú defines `LOCATION`; `DROP` solo borra
metadatos. Para EduStream — managed: los datos son exclusivos de la plataforma y
delegar el ciclo de vida a Unity Catalog simplifica governance. External solo
tendría sentido si un sistema externo consumiera los Parquet directamente.

**2 · Versiones acumuladas y VACUUM.** Cada write retiene archivos nuevos. Sin
limpieza: small files problem (planning overhead supera la ejecución) y costo de
storage lineal con la historia. `VACUUM` elimina archivos no referenciados fuera
del retention window (7 días default). `OPTIMIZE` compacta pero no borra versiones
— son complementarios.

**3 · expect / expect_or_drop / expect_or_fail.** `expect_or_drop` para pagos
negativos (corruptos, se descartan). `expect` para `course_id` nulo (estado
transitorio posible, se conserva marcado). `expect_or_fail` cuando la violación
invalida el batch entero — en el notebook DLT se usa para detectar schema drift
(`_rescued_data IS NULL`): si la fuente cambió su estructura, fail-fast antes de
producir métricas sobre datos mal parseados.

**4 · Estrategia de ZORDER.** `enrolled_at` para `silver_enrollments` (queries
temporales: cohortes, retención). `course_id` para gold (queries por curso).
Principio: ZORDER por la columna más frecuente en `WHERE`/`GROUP BY` del query
history real. Liquid Clustering (`CLUSTER BY`) es el reemplazo moderno — se adapta
sin reescribir la tabla; ver ADR-05 en el design doc.

---

*DataHackers Academy · AI Data Engineer Bootcamp · Sesión 06*
