# Documento de diseño técnico — EduStream Pipeline

> Pipeline de datos enterprise sobre la plataforma educativa EduStream.
> Este documento registra las decisiones de arquitectura y sus trade-offs.
> Audiencia: data engineers que mantengan o extiendan el pipeline.

---

## 1. Contexto y objetivo

EduStream es una plataforma de cursos online. El negocio necesita responder, de
forma confiable y reproducible, preguntas como: ¿qué cursos tienen mejor tasa de
completación?, ¿cuánto revenue genera cada categoría?, ¿cómo evoluciona el precio
de un curso en el tiempo?

El reto no es calcular esas métricas una vez — es construir un pipeline que las
calcule **correctamente en cada ejecución**, que tolere datos sucios, que sea
re-ejecutable sin efectos secundarios, y cuya calidad sea auditable.

### Requisitos no funcionales

| Requisito | Cómo se cumple |
|-----------|----------------|
| Idempotencia | `MERGE INTO` en silver; re-ejecutar no duplica ni corrompe |
| Trazabilidad | Columnas de linaje (`_pipeline_run`, `_ingested_at`) + tabla `_dq_audit` |
| Tolerancia a datos sucios | DQ gating por capa; `rescuedDataColumn` para schema drift |
| Historización | SCD Type 2 en `dim_courses` |
| Reproducibilidad | Datos sintéticos con `seed=42`; schema explícito (data contract) |
| Testabilidad | Suite de 10 tests de lógica ejecutable fuera de Databricks |

---

## 2. Arquitectura

Arquitectura medallion de tres capas, más una dimensión SCD2 transversal.

```
landing (Volume, CSV)
    │  Auto Loader + schema explícito + rescuedDataColumn
    ▼
BRONZE  ─ mirror inmutable del raw, con linaje
    │  limpieza · deduplicación · DQ gating
    ▼
SILVER  ─ datos validados y conformados
    │         │
    │         └──▶ DIM_COURSES (SCD2) ─ historización de cambios
    │  agregación de negocio
    ▼
GOLD  ─ métricas listas para consumo (BI, reportes)
```

Cada capa tiene una responsabilidad única y no la cruza:

- **Bronze** nunca transforma. Es un espejo del origen con metadatos de linaje.
  Si algo sale mal aguas abajo, bronze es la fuente de verdad para reprocesar.
- **Silver** limpia, deduplica y valida. Aquí viven las reglas de negocio sobre
  registros individuales (pago nulo → 0, descartar negativos).
- **Gold** agrega. Sin lógica de limpieza — si gold necesita limpiar algo, es
  señal de que silver está incompleto.

---

## 3. Decisiones de arquitectura (ADR)

### ADR-01 · MERGE en silver en lugar de overwrite

**Decisión.** `silver_enrollments` se actualiza con `MERGE INTO`, no con
`mode("overwrite")`.

**Contexto.** El entregable base usaba overwrite en todas las capas. Funciona en
un laboratorio con un único CSV estático, pero tiene tres problemas en producción:

1. *No es incremental* — reconstruye toda la tabla en cada corrida. Con un CSV de
   5 mil filas no se nota; con cargas diarias acumuladas durante un año, sí.
2. *Concurrencia* — si dos cargas corren a la vez, una pisa a la otra.
3. *Semántica pobre del historial* — cada overwrite es una versión "todo o nada".

**Trade-off.** `MERGE` exige una clave de negocio estable (`enrollment_id`) y un
paso previo de deduplicación del batch entrante. Es más código. A cambio se gana
idempotencia real: re-ejecutar la celda N veces deja la tabla idéntica — propiedad
verificada en `tests/test_pipeline_logic.py::test_upsert_idempotente`.

**Por qué bronze sí usa overwrite.** Bronze es deliberadamente un espejo del raw.
Reprocesarlo entero es la semántica correcta — no se hace upsert contra una fuente
que se re-lee completa.

---

### ADR-02 · Schema explícito en lugar de inferSchema

**Decisión.** Cada tabla bronze se lee con un `StructType` declarado, no con
`inferSchema=True`.

**Contexto.** `inferSchema` lee los datos dos veces (una para inferir, una para
cargar) y, peor, el tipo inferido depende de los datos: un CSV donde
`payment_amount` trae solo enteros infiere `int`; el siguiente con un decimal
infiere `double`. El pipeline rompe sin que nadie cambie una línea de código.

**Trade-off.** Mantener los schemas declarados es trabajo manual y hay que
actualizarlos si la fuente cambia legítimamente. A cambio, el schema se vuelve un
**contrato**: si la fuente lo viola, el pipeline falla rápido y de forma visible,
en vez de producir datos incorrectos en silencio. El `rescuedDataColumn` captura
las columnas inesperadas sin tumbar la carga.

---

### ADR-03 · SCD Type 2 para la dimensión de cursos

**Decisión.** Los cursos se modelan como una dimensión SCD2 (`dim_courses`), no
como una tabla de estado actual.

**Contexto.** Los atributos de un curso cambian: sube el precio, se recategoriza,
cambia de instructor. Una tabla que solo guarda el estado actual no puede
responder "¿cuánto costaba este curso cuando se inscribió el usuario X en marzo?".

**Cómo funciona.** Cada fila de `dim_courses` representa una versión de un curso
durante un intervalo `[valid_from, valid_to]`. La versión vigente tiene
`is_current = true` y `valid_to = null`. Cuando llega un cambio:

1. Se calcula un hash SHA-256 de los atributos rastreados.
2. Si el hash difiere del de la versión vigente, esa versión se cierra
   (`is_current = false`, `valid_to = ayer`).
3. Se inserta la versión nueva como vigente.

**Trade-off.** SCD2 multiplica las filas (un curso con 5 cambios = 5 filas) y
exige que las queries de gold filtren `is_current = true` para el estado actual,
o hagan un join temporal para el estado histórico. Es más complejo que una tabla
plana. A cambio, la historia es recuperable — y para una plataforma que cobra,
saber el precio vigente en cada momento no es opcional.

**Alternativa descartada.** SCD Type 1 (sobrescribir) es más simple pero pierde la
historia. Delta time travel *parece* una alternativa, pero no lo es: time travel
versiona la *tabla entera*, no la *entidad de negocio*, y el retention de VACUUM
lo borra. SCD2 es historización a nivel de dato, permanente.

---

### ADR-04 · DQ gating con tabla de auditoría

**Decisión.** La calidad de datos se valida con una función `run_dq_check` que
registra cada chequeo en una tabla `_dq_audit`, y puede bloquear el pipeline.

**Contexto.** Validar calidad con `assert` dispersos en el código tiene dos fallas:
los resultados no quedan en ningún lado, y no hay un criterio uniforme de cuándo
detener el pipeline.

**Cómo funciona.** Cada chequeo recibe una condición booleana y una severidad:

- `severity="warn"` — registra el resultado, no detiene nada.
- `severity="block"` — si el `pass_rate` cae por debajo del 95%, lanza una
  excepción y detiene el pipeline.

Todo queda en `_dq_audit` con el `run_id`, así que la salud del pipeline es una
consulta SQL, no una lectura de logs.

**Relación con las DLT expectations.** El pipeline DLT (notebook aparte) usa
`@dlt.expect` / `@dlt.expect_or_drop`. `run_dq_check` es el equivalente para el
notebook batch: mismo concepto (gating declarativo + métricas), aplicado donde DLT
no llega.

---

### ADR-05 · ZORDER hoy, Liquid Clustering como evolución

**Decisión.** Se usa `OPTIMIZE ... ZORDER BY` por ser universal en Databricks Free
Edition, documentando que **Liquid Clustering** es la dirección preferida cuando
esté disponible.

**Estrategia de clustering por tabla:**

| Tabla | Columna | Razón |
|-------|---------|-------|
| `silver_enrollments` | `enrolled_at` | Las queries filtran por rango de fechas (cohortes, retención) |
| `gold_course_performance` | `course_id` | Las queries filtran/agrupan por curso |

**Por qué no particionar por fecha.** Particionar físicamente `silver_enrollments`
por `enrolled_at` parece natural, pero con un dataset de este tamaño produciría
cientos de particiones diminutas — el small files problem, agravado. El
particionamiento físico solo conviene en tablas grandes (cientos de GB) con una
columna de baja cardinalidad. Aquí, ZORDER (data skipping sin particiones físicas)
es la opción correcta.

**ZORDER vs Liquid Clustering.** ZORDER exige elegir el orden de las columnas y
recalcula sobre toda la tabla en cada `OPTIMIZE`. Liquid Clustering (`CLUSTER BY`)
se adapta incrementalmente, no exige orden de columnas, y permite cambiar las
claves de clustering sin reescribir. Es el reemplazo moderno de ZORDER. Se deja
como evolución natural del pipeline.

---

## 4. Modelo de datos

### Tablas del pipeline

| Capa | Tabla | Grano | Clave |
|------|-------|-------|-------|
| Bronze | `bronze_enrollments` | 1 fila por registro raw | — |
| Bronze | `bronze_courses` | 1 fila por registro raw | — |
| Bronze | `bronze_progress` | 1 fila por registro raw | — |
| Bronze | `bronze_instructors` | 1 fila por registro raw | — |
| Silver | `silver_enrollments` | 1 fila por inscripción | `enrollment_id` |
| Silver | `silver_progress` | 1 fila por (usuario, curso) | `user_id`+`course_id` |
| Dim | `dim_courses` | 1 fila por (curso, versión) | `course_sk` |
| Gold | `gold_course_performance` | 1 fila por curso | `course_id` |
| Gold | `gold_revenue_by_category` | 1 fila por categoría | `category` |
| Audit | `_dq_audit` | 1 fila por chequeo de DQ | — |

### Columnas de linaje (convención)

Toda tabla bronze/silver lleva columnas con prefijo `_`:

- `_ingested_at` — timestamp de ingestión a bronze
- `_processed_at` — timestamp de procesamiento a silver
- `_pipeline_run` — id de la corrida (`YYYYMMDD_HHMMSS`)
- `_source_file` — archivo de origen
- `_rescued_data` — datos que no encajaron en el contrato (idealmente null)

---

## 5. Calidad de datos

### Chequeos implementados

| Capa | Chequeo | Severidad | Acción si falla |
|------|---------|-----------|-----------------|
| Silver | `enrollment_id_unico` | block | Detiene el pipeline |
| Silver | `pago_no_negativo` | warn | Registra; las filas se filtran |
| Silver | `completion_pct_en_rango` | block | Detiene el pipeline |

### Imperfecciones del dataset y su manejo

El generador de datos sintéticos inyecta deliberadamente datos sucios para que el
pipeline demuestre que los maneja:

| Imperfección | % aprox. | Capa que la maneja | Tratamiento |
|--------------|----------|--------------------|-----------  |
| `payment_amount` nulo | 8% | Silver | `coalesce` → 0.0 |
| `payment_amount` negativo | 3% | Silver | DQ warn + filtrado |
| `course_id` nulo | 4% | DLT / Silver | `expect` marca, no descarta |
| `enrollment_id` duplicado | 2% | Silver | Dedup por window + MERGE |
| `total_lessons` = 0 | 5% | Silver | Filtrado (evita div/0) |

---

## 6. Cómo extender el pipeline

- **Nueva tabla bronze** — agregar su schema a `SCHEMAS` y llamar `ingest_bronze`.
- **Nuevo chequeo de DQ** — una línea de `run_dq_check` con la condición y la
  severidad. Queda registrado en `_dq_audit` automáticamente.
- **Nueva métrica gold** — nueva tabla que lea de silver/dim. No agregar lógica de
  limpieza en gold; si hace falta, va en silver.
- **Nuevo atributo SCD2** — agregarlo a la lista `tracked`. El hash lo incluye
  automáticamente y los cambios se historizan.

---

## 7. Limitaciones conocidas

- El pipeline es **batch**. El notebook DLT (aparte) demuestra el patrón streaming
  con Auto Loader, pero la ruta principal asume cargas batch.
- En Databricks Free Edition no hay scheduler de producción; la orquestación real
  requeriría Databricks Workflows o un orquestador externo (Airflow, Dagster).
- Los datos son sintéticos. Contra datos reales habría que revalidar los umbrales
  de DQ (el 95% del gating es un punto de partida, no un valor universal).
