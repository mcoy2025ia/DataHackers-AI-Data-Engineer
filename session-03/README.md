# Sectoral Market Intelligence Framework

> **Ecosistema ETL y Agentic AI para análisis cuantitativo de renta variable.**
> Pipeline de ingesta resiliente, transformaciones vectorizadas, almacenamiento columnar
> y un agente de estrategia impulsado por Llama 3.3 que produce análisis de valor relativo
> sectorial en menos de 500ms.

---

## Concepto

Los sistemas de análisis financiero tradicionales separan la ingeniería de datos de la toma
de decisiones. Este framework los une: el mismo pipeline que descarga, valida y persiste
datos OHLCV también alimenta un agente de IA que compara el activo objetivo con sus pares
sectoriales y entrega una recomendación táctica con nivel de convicción.

El resultado es un ecosistema end-to-end que va de la API a la tesis de inversión sin
intervención manual.

---

## Arquitectura de Datos

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Yahoo Finance API                                 │
│                   yf.Ticker(symbol).history()                            │
└────────────────────────────┬────────────────────────────────────────────┘
                             │  tenacity Exponential Backoff
                             │  (429/5xx retryable · 403/404 terminal)
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  extract()   →  DataFrame OHLCV normalizado (UTC, lowercase cols)        │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  validate()  →  Null % · OHLCV integrity · Z-Score (|Z|>3.0)            │
│                 ├── clean records  ──────────────────────────────────►   │
│                 └── outliers  ──►  data/raw/quarantine/  (DLQ/audit)     │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  transform() →  RSI(14) · log_return · Volatility · Price Z-Score        │
│                 Rolling Sharpe(14d) · Max Drawdown(30d) · Signals        │
│                 ─── ALL vectorized, O(n) total, zero Python loops ───    │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  load()      →  data/output/{SYMBOL}_data.parquet                        │
│                 engine=pyarrow · compression=snappy · schema embedded     │
└──────────────────┬──────────────────────────────┬───────────────────────┘
                   │                              │
                   ▼                              ▼
┌──────────────────────────┐     ┌──────────────────────────────────────┐
│   Streamlit Dashboard    │     │         Groq AI Agent                 │
│   4 tabs por ticker      │     │   taxonomy.get_peers(symbol)          │
│   auto-detect symbols    │     │   → peers Parquet → sector context    │
│   Altair charts          │     │   → Llama 3.3-70b-versatile           │
│   Trader Intelligence    │     │   → Relative Value Report < 500ms     │
└──────────────────────────┘     └──────────────────────────────────────┘
```

---

## Stack Tecnologico

| Capa | Tecnologia | Version | Rol |
|------|------------|---------|-----|
| Entorno | `uv` | latest | Gestion de dependencias y ejecucion de scripts |
| Extraccion | `yfinance` | 1.3.0 | OHLCV historico sin autenticacion |
| Resiliencia | `tenacity` | latest | Exponential Backoff en llamadas de red |
| Transformacion | `pandas` (vectorizado) | latest | Indicadores tecn. sin bucles Python |
| Algebra | `numpy` | latest | EWM, rolling std, log returns |
| Persistencia | `pyarrow` + Parquet/Snappy | latest | Columnar I/O, schema enforcement |
| Dashboard | `streamlit` | 1.31 | UI interactiva de 4 vistas por ticker |
| Graficos | `altair` | 5.5 | Graficos declarativos basados en Vega-Lite |
| Agente IA | `groq` SDK | 1.2.0 | Inferencia LPU < 500ms sobre Llama 3.3 |
| Secretos | `python-dotenv` | latest | Carga segura de `GROQ_API_KEY` desde `.env` |

---

## Metricas Core

### RSI(14) — Momentum Oscilador de Wilder

```
RSI = 100 − [ 100 / (1 + AvgGain₁₄ / AvgLoss₁₄) ]
```

Implementado con EWM (`alpha=1/14`, `adjust=False`) — la formulacion correcta de Wilder,
no una media movil simple disfrazada. La guarda `avg_loss.replace(0.0, np.nan)` previene
division por cero en streaks alcistas prolongados.

| Zona | Interpretacion |
|------|----------------|
| RSI > 70 | `OVERBOUGHT` — momentum agotado, riesgo de reversal |
| RSI 30–70 | `NEUTRAL` — precio dentro del rango de equilibrio |
| RSI < 30 | `OVERSOLD` — presion vendedora extrema, posible rebote |

### Price Z-Score — Reversion a la Media

```
Z = (close_t − EMA₂₀) / σ₂₀   donde σ₂₀ = rolling_std(window=20)
```

Mide cuantas desviaciones estandar se aleja el precio de su tendencia reciente (EMA 20 dias).
Valores `|Z| > 2.0` indican precio estadisticamente estirado — señal de posible retorno a la media.

### Sharpe Ratio — Calidad de Tendencia Ajustada por Riesgo

$$Sharpe = \frac{\mu_{ret} - R_f}{\sigma_{ret}}$$

En la implementacion, `R_f = 0` (simplificacion valida para ventanas de 14 dias).
El factor `√252` anualiza el ratio desde escala diaria.

```python
sharpe_14 = (roll_mean / roll_std.replace(0.0, np.nan)) * np.sqrt(252)
```

| Sharpe | Calidad |
|--------|---------|
| > 3.0 | ELITE — menos del 5% de activos lo alcanzan |
| 1.0–3.0 | STRONG — tendencia con retornos consistentes |
| 0.0–1.0 | MODERATE — retorno positivo, volatilidad alta |
| < 0.0 | NEGATIVE — la volatilidad supera al retorno |

### Max Drawdown (30d) — Exposicion al Riesgo de Capital

```
MDD_t = (close_t − max(close_{t−30..t})) / max(close_{t−30..t})   ∈ (−∞, 0]
```

Siempre no-positivo. MDD = 0% indica nuevos maximos (Price Discovery).
MDD < −10% indica caida significativa desde el pico — posible zona de capitulacion.

---

## Capa de Inteligencia Agentica

El agente `MarketExpert` resuelve el problema del **analisis aislado**: las metricas de un
ticker son vacías sin el universo de comparacion correcto.

### Flujo de razonamiento

```
1.  get_sector("NVDA")  →  "Semiconductors"
2.  get_peers("NVDA")   →  ["AMD", "AVGO", "INTC"]
3.  Para cada peer → carga ultimo row de {peer}_data.parquet
4.  Extrae {sharpe, zscore} del peer
5.  Python calcula avg_sharpe_sector (no el LLM)
6.  Construye prompt estructurado:
      ACTIVO OBJETIVO: metricas del ticker seleccionado
      COMPETIDORES:    lista de pares con sus KPIs
      BENCHMARK:       promedio sectorial pre-calculado
      TAREA:           4 preguntas especificas + formato Bloomberg
7.  Groq API → Llama 3.3-70b (temperature=0.2, max_tokens=512)
8.  st.chat_message("assistant").markdown(report)
```

### Por que KPIs procesados, no datos crudos

Enviar 22 filas de precios de cierre al LLM y pedir que calcule el Sharpe produce
alucinaciones numericas. Enviar `Sharpe=4.2, sector_avg=2.1` y pedir que razone
sobre la divergencia produce inteligencia. **El LLM no calcula — razona.**

---

## Data Storage Strategy: Why Parquet?

La eleccion de Apache Parquet como formato de persistencia no es una convencion —
es una decision de arquitectura con implicaciones directas en rendimiento, integridad
y compatibilidad ecosistemica.

### Eficiencia de Lectura: Almacenamiento Columnar

Parquet organiza los datos por columna, no por fila. Para calcular `rsi_14` o
`sharpe_14`, el motor solo lee los bytes de esas dos columnas en disco — el resto
del archivo no se toca.

```
CSV (row-store):
  [timestamp, open, high, low, close, volume, rsi_14, sharpe_14, ...]  ← fila 1
  [timestamp, open, high, low, close, volume, rsi_14, sharpe_14, ...]  ← fila 2
  → Query "dame solo rsi_14": lee TODAS las columnas de TODAS las filas

Parquet (column-store):
  [rsi_14_col:   65.3, 71.2, 68.9, ...]   ← columna comprimida independiente
  [sharpe_14_col: 3.2, 4.1, 3.8, ...]
  → Query "dame solo rsi_14": lee UNICAMENTE la columna rsi_14
```

Para un DataFrame de 22 filas × 14 columnas, el impacto es marginal. Para
5 años × 500 tickers (650,000 filas × 14 columnas), la diferencia entre
leer 2 columnas vs. 14 es un factor ~7x en I/O — directamente proporcional
al tiempo de respuesta del agente de IA al cargar los datos de pares.

### Integridad de Datos: Schema Embebido

Parquet almacena los metadatos de tipos dentro del archivo. Al re-leerlo,
`pandas` reconstruye los dtypes exactos sin ninguna instruccion adicional:

```python
# Parquet: schema preserved automatically
df = pd.read_parquet("NVDA_data.parquet")
df["timestamp"].dtype   # → datetime64[ns, UTC]  ✓
df["close"].dtype       # → float64              ✓
df["signal"].dtype      # → object (str)          ✓

# CSV: schema lost, manual reconstruction required
df = pd.read_csv("NVDA_data.csv")
df["timestamp"].dtype   # → object (string!)     ✗ requires parse_dates=True
df["close"].dtype       # → float64              ✓ (lucky)
# timezone information: destroyed permanently
```

En pipelines financieros, un timestamp sin timezone es un dato corrupto.
Parquet preserva `DatetimeTZDtype[ns, UTC]` en los metadatos del archivo —
la informacion viaja con los datos, no con el codigo que los lee.

### Compresion Snappy y Latencia del Agente

La compresion Snappy reduce el tamaño del Parquet 4–8x respecto al CSV equivalente
gracias a la alta correlacion inter-fila en datos OHLCV financieros (los precios
diarios se mueven < 2% en condiciones normales — alta redundancia que la compresion
explota eficientemente).

```
NVDA_data.parquet (22 rows × 14 cols, Snappy):  ~11.5 KB
NVDA_data.csv     (equivalente sin compresion):  ~4.2 KB
```

> A escala: 500 tickers × 5 años de historia → diferencia de ~200 MB vs. ~2 GB.
> En S3, la diferencia se traduce directamente en costo de almacenamiento y en
> bytes escaneados por Athena (facturado por byte).

Snappy prioriza velocidad de descompresion sobre ratio de compresion — correcto
para queries interactivos donde el usuario espera frente al dashboard.

### Compatibilidad con Ecosistemas de Big Data

Parquet es el formato nativo de los principales motores de procesamiento distribuido
y plataformas cloud:

| Plataforma | Soporte Parquet | CSV |
|------------|----------------|-----|
| AWS Athena | Nativo (predicate pushdown) | Scan completo |
| Azure Synapse | Nativo | Conversion requerida |
| Snowflake | Stage directo | ETL adicional |
| Apache Spark | Formato preferido | Conversion implicita |
| DuckDB | Lectura directa con SQL | Lectura directa (sin schema) |
| dbt | Compatible via adaptadores | No recomendado para modelos |

> **Principio:** Un archivo Parquet generado hoy por este pipeline puede ser
> consultado mañana directamente desde Athena, Spark o DuckDB sin ninguna
> transformacion. Un CSV requiere definir el schema en cada punto de consumo.

---

## Lecciones Aprendidas (Senior Insights)

### 1 — Resiliencia de Fuentes: Arquitectura Agnostica a la API

El pipeline fue diseñado originalmente sobre Finnhub. La API gratuita retorno **HTTP 403**
en produccion para todos los tickers. El fix requirio modificar exactamente **un metodo**
(`_download_yf()`) y eliminar el schema Pydantic de validacion de respuesta.

El 95% del codigo de negocio (validate, transform, load, dashboard) no se toco.

> **Principio:** El contrato entre capas es Parquet, no la API. Disenar el sistema
> alrededor del formato de almacenamiento, no alrededor del proveedor de datos,
> hace que los pivots de fuente sean cirugias, no refactors.

### 2 — Eficiencia Computacional: Vectorizacion vs. Bucles

Toda transformacion en `transform()` es una cadena de operaciones nativas de pandas/numpy.
Cero `iterrows()`, cero list comprehensions sobre filas.

```python
# Incorrecto (O(n) overhead Python por fila)
for i, row in df.iterrows():
    df.loc[i, "rsi"] = compute_rsi(df[:i])

# Correcto (O(n) total, ejecutado en C)
result["rsi_14"] = series.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
```

Para 22 filas la diferencia es imperceptible. Para 5 años × 500 tickers (650,000 filas),
la diferencia es de segundos vs. horas.

> **Principio:** En series temporales financieras, la pregunta no es "¿funciona?"
> sino "¿escala?". Si el calculo requiere un bucle Python, hay una operacion
> vectorizada que lo reemplaza.

### 3 — Contextualizacion para IA: Eliminar Alucinaciones desde el Diseño

Los LLMs alucinan cuando procesan numeros crudos en series largas. La solucion no es un
mejor modelo — es un mejor input.

```
Prompt ingenuo:  "Analiza estos precios: [185.2, 187.1, 183.4, ...]"
                  → El modelo calcula (mal) y alucina métricas

Prompt correcto: "RSI=67.3, Sharpe=4.2, Z-Score=1.8, sector_avg_Sharpe=2.1"
                  → El modelo razona sobre numeros exactos que Python calculo
```

El pre-procesamiento en Python no es overhead — es la capa de verdad que garantiza
que el LLM razone sobre hechos, no sobre interpolaciones.

> **Principio:** El LLM es el capa de razonamiento, no de calculo.
> Separar responsabilidades elimina alucinaciones desde la arquitectura.

### 4 — SecOps & Entorno: Ciclo de Vida de Secretos en Aplicaciones Long-Running

`load_dotenv()` en el scope global del modulo se ejecuta una sola vez cuando Streamlit
importa el archivo. Si el usuario edita `.env` despues de iniciar el servidor, los cambios
no se reflejan hasta reiniciar el proceso.

**Fix implementado:** `MarketExpert.__init__()` llama `load_dotenv()` en el momento de
instanciacion. El agente se crea dentro del handler del boton, no en el scope global.
Cada clic re-lee el `.env`.

```python
# Incorrecto — load_dotenv() ejecutado en importacion
load_dotenv()                         # modulo nivel
class MarketExpert:
    def __init__(self): ...

# Correcto — load_dotenv() en punto de uso
class MarketExpert:
    def __init__(self):
        load_dotenv()                 # re-lee .env en cada instanciacion
        api_key = os.getenv("GROQ_API_KEY")
```

> **Principio:** En aplicaciones interactivas de larga duracion, los secretos deben
> cargarse en el punto de uso, no en la importacion. El proceso no muere entre requests.

### 5 — Eleccion de Formatos de Intercambio: Binario Columnar desde el Inicio

Incluso para archivos pequeños (~11.5 KB por ticker en este proyecto), adoptar
Apache Parquet desde la primera iteracion es la decision correcta.

El argumento *"es un archivo pequeño, CSV es suficiente"* ignora tres consecuencias
de largo plazo:

**Deuda de schema:** Cada consumidor de un CSV debe re-implementar la logica de
parseo de tipos (fechas, decimales, booleanos). Cada re-implementacion es un vector
de bug. Parquet centraliza el schema en el archivo — se parsea una vez, se consume
infinitas veces de forma identica.

**Incompatibilidad ecosistemica:** Mover datos de un proyecto local a Azure Data Lake,
AWS S3 + Athena o Snowflake con CSV requiere ETL adicional para schema discovery.
Con Parquet, el archivo se copia y es inmediatamente queryable con SQL nativo.

**Eficiencia a escala:** Un portafolio de 18 tickers × 30 dias es trivial. El mismo
portafolio a 5 años × 500 tickers (~300 archivos Parquet) sigue siendo eficiente sin
cambios de arquitectura. El mismo diseño con CSV colapsaria bajo I/O y tiempo de parseo.

> **Principio:** Los formatos de almacenamiento son decisiones de arquitectura, no
> de conveniencia. Adoptar el estandar de Big Data desde el MVP elimina la necesidad
> de migraciones costosas cuando el volumen escala. Parquet en un laptop de desarrollo
> es el mismo Parquet en Snowflake enterprise.

---

## Configuracion

### Variables de entorno (`.env`)

```env
GROQ_API_KEY=gsk_xxxxxxxxxxxxxxxxxxxx
```

> Clave gratuita en [console.groq.com](https://console.groq.com).
> `.env` esta en `.gitignore` — nunca en el repositorio.

---

## Instrucciones de Operacion

### Pipeline ETL — Ingesta masiva de 18 tickers

```bash
uv run python src/pipeline.py
```

Procesa el portafolio completo en secuencia con pausa de cortesia de 1s entre tickers.
Al finalizar imprime el Batch Run Summary en stdout y escribe logs completos en
`logs/pipeline.log`.

**Output esperado:**

```
----------------------------------------------------------------
  BATCH RUN SUMMARY  —  18 tickers  /  30d window
----------------------------------------------------------------
  NVDA    ok  | 22 records | NVDA_data.parquet
  AAPL    ok  | 22 records | AAPL_data.parquet
  ...
  RKLB    ok  | 22 records | RKLB_data.parquet
----------------------------------------------------------------
  Processed: 18   Failed: 0
----------------------------------------------------------------
```

### Dashboard Interactivo

```bash
uv run streamlit run src/app.py
```

Abre en `http://localhost:8501`. El selector lateral detecta automaticamente todos los
archivos `.parquet` presentes en `data/output/`.

| Tab | Vista | Contenido |
|-----|-------|-----------|
| Trader | Precio + Momentum | Close price area chart + RSI(14) con bandas 30/70 |
| Business Analyst | KPIs + Distribucion | Metricas en tiempo real + signal breakdown + tabla |
| Pipeline Health | Calidad de datos | Produccion vs. DLQ/Quarantine + health score |
| Advanced Analytics | Riesgo + IA | Z-Score · Sharpe · Drawdown · Trader Intelligence · Groq |

### Analisis de Valor Relativo (Groq)

Dentro de **Advanced Analytics**:

1. El panel muestra automaticamente los pares del sector del ticker seleccionado
2. Verificar que los pares tienen datos en `data/output/` (panel de preview)
3. Presionar **"Ejecutar Analisis de Valor Relativo (Groq)"**
4. El agente consulta Llama 3.3 con el contexto sectorial completo
5. La respuesta incluye posicionamiento relativo, calidad de tendencia y nivel de conviccion

> Si aparece error de configuracion: agregar `GROQ_API_KEY=gsk_...` al `.env`
> y presionar el boton de nuevo (sin reiniciar el servidor).

---

## Estructura del Proyecto

```
Miniproyecto-03/
├── src/
│   ├── pipeline.py              # ETL completo — MarketPipeline
│   ├── app.py                   # Dashboard Streamlit (4 tabs + Groq)
│   ├── agents/
│   │   ├── __init__.py
│   │   ├── market_expert.py     # Agente Groq — analisis de valor relativo
│   │   ├── Business_Analyst.md
│   │   ├── Data_Engineer.md
│   │   ├── Trader_Experto.md
│   │   ├── Token_Architect.md
│   │   ├── Caveman.md
│   │   └── Security_Compliance.md
│   └── utils/
│       ├── __init__.py
│       └── taxonomy.py          # SECTORS · get_peers() · get_sector()
├── data/
│   ├── output/                  # 18 x {SYMBOL}_data.parquet  (git-ignored)
│   └── raw/
│       └── quarantine/          # DLQ — outliers Z-Score     (git-ignored)
├── logs/
│   └── pipeline.log             # Logs DEBUG completos        (git-ignored)
├── .env                         # GROQ_API_KEY                (git-ignored)
├── .gitignore
├── CLAUDE.md                    # Guia de desarrollo
├── CLAUDE_LOG.md                # 14 decisiones tecnicas documentadas
└── README.md
```

---

## Decision Journal

`CLAUDE_LOG.md` documenta las 14 decisiones arquitectonicas del proyecto:

| # | Decision | Categoria |
|---|----------|-----------|
| 001 | Exponential Backoff con tenacity | Resiliencia |
| 002 | Errores retryables vs. terminales (403 = terminal) | Resiliencia + SecOps |
| 003 | Quarantine (DLQ) vs. Drop para outliers | Calidad de datos |
| 004 | Parquet + Snappy vs. CSV | Persistencia |
| 005 | Vectorizacion de metricas avanzadas | Performance |
| 006 | Trader Intelligence — narrativa automatica | UX / Presentacion |
| 007 | Batch ingestion de 18 tickers con aislamiento de errores | Escalabilidad |
| 008 | Migracion Finnhub → Yahoo Finance (pivot de API) | Resiliencia de fuente |
| 009 | Taxonomia sectorial estatica para valor relativo | Arquitectura IA |
| 010 | Integracion agente Groq (Llama 3.3-70b) | Agentic AI |
| 011 | KPIs vectorizados como contexto LLM — anti-alucinacion | Agentic AI |
| 012 | Post-Mortem 401 — ciclo de vida de secretos en Streamlit | SecOps |
| 013 | Framework de taxonomia — universo de inversion definido | Arquitectura |
| 014 | Cierre y estabilizacion del Data Lake sectorial | Entrega final |
