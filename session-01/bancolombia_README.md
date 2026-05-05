# Análisis de arquitectura de datos: Bancolombia

**Autor**: Manuel Alberto Coy Benavides  
**Fecha**: Mayo 2026  
**Bootcamp**: AI Data Engineer Bootcamp · DataHackers Academy · Sesión 1

---

## 1. La empresa

Bancolombia es el banco privado más grande de Colombia y la institución financiera líder de América Latina por activos en su país de origen. Opera en los segmentos de banca de consumo, banca comercial, banca corporativa, corretaje de valores, arrendamiento financiero, factoring, fiducia y gestión de activos. Su presencia internacional abarca **Colombia, Panamá, El Salvador, Guatemala, Puerto Rico y Perú**, dentro del perímetro del holding **Grupo Cibest** (nueva estructura corporativa desde 2024).

En cifras oficiales recientes: más de **33 millones de clientes regionales** (holding), **16,2 millones de clientes en la operación local**, **9,4 millones de clientes digitales mensuales** y **4.184 millones de transacciones monetarias en 2025**. En empleo, la operación local reportó **23.649 empleados directos** (Informe de Gestión 2025). Su core bancario está siendo reemplazado por **COREX**, una plataforma de cuarta generación basada en **Thought Machine Vault**, diseñada para operar en nube con arquitectura modular y orientada a eventos. El **70% de las transacciones monetarias del sistema financiero colombiano circulan por Bancolombia**, convirtiendo el dato en activo estratégico de primer orden.

---

## 2. Casos de uso de datos críticos

| # | Caso de uso | Criticidad | Latencia requerida |
|---|---|---|---|
| 1 | **Fraude transaccional y account takeover** — detectar y bloquear operaciones anómalas y suplantaciones antes de que generen pérdida. Incluye biometría facial, señales de dispositivo y comportamiento histórico. | Alta | Tiempo real < 200ms |
| 2 | **Scoring crediticio con Open Finance** — mejorar aprobación, pricing y calidad de cartera usando datos internos y datos consentidos de otras entidades bajo el esquema de finanzas abiertas regulado por la SFC. | Alta | Near real-time para decisión; batch para entrenamiento |
| 3 | **Personalización, next best action y prevención de churn** — activar ofertas y journeys relevantes en canal digital usando propensión, voz del cliente (VOCE) y eventos de app. | Alta | Near real-time en canal; batch diario/semanal para recalibración |
| 4 | **Interoperabilidad de pagos — QR, llaves y Bre-B** — dar trazabilidad, conciliación y confirmación inmediata de pagos interoperables para personas y comercios. Sistema ya activo con 400M txns en 2025. | Alta | Tiempo real |
| 5 | **Gobierno, privacidad y reporting regulatorio multipaís** — cumplir políticas de tratamiento de datos, reserva bancaria, finanzas abiertas y reportes auditables en 6 jurisdicciones. | Alta (regulatoria) | Batch diario, intradía y cierres mensuales |

---

## 3. Fuentes de datos identificadas

| Fuente | Tipo | Volumen estimado | Cómo se ingesta | Uso principal |
|---|---|---|---|---|
| Core bancario COREX (Thought Machine Vault) | Transaccional + eventos | 4.184 millones txns monetarias/año **[oficial]** | CDC + eventos del core orientado a eventos **[inferencia]** | Riesgo, finanzas, conciliación, customer 360 |
| Mi Bancolombia + Sucursal Virtual Personas/Negocios | Eventos + transaccional | +250 millones txns digitales/mes **[oficial]** | SDKs, API Gateway, stream de eventos **[inferencia]** | Analítica digital, personalización, observabilidad |
| Pagos QR + llaves + Bre-B (interoperabilidad) | Transaccional + streaming | 400 millones txns en 2025 **[oficial]** | APIs, cola de eventos, rieles de confirmación RT **[inferencia]** | Pagos, conciliación, merchant analytics, fraude |
| Open Finance — APIs a terceros y consentimientos | API + datos externos | No público **[estimado]** | API Gateway, OAuth, store de consentimientos **[inferencia]** | Originación, agregación financiera, productos conectados |
| Tabot + atención digital al cliente | Conversacional + no estructurado | No público **[estimado]** | Logs conversacionales, CRM, object storage **[inferencia]** | Autoservicio, análisis de intención, experiencia |
| Biometría y monitoreo antifraude | Eventos de seguridad | +700.000 habilitaciones/mes en un flujo concreto **[oficial]** | Streaming desde autenticación, motores analíticos **[inferencia]** | Prevención fraude, account recovery, seguridad |
| VOCE y encuestas embebidas en canal | Feedback + experiencia | No público **[estimado]** | Captura embebida, eventos de experiencia, batch **[inferencia]** | NPS, cierre de fricciones, churn, diseño de journeys |
| Trámites digitales y documentos (AIO) | Documental + no estructurado | +40 tipos de solicitudes digitales **[oficial]** | Carga documental, OCR/NLP, pipelines AIO **[inferencia]** | Automatización documental, eficiencia operativa |
| CRM corporativo | Datos maestros + interacciones | 33M registros cliente **[oficial]** | Batch diario + CDC **[inferencia]** | Segmentación, campañas, 360° cliente |
| Logs infraestructura AWS (EKS, RDS, DynamoDB) | Logs técnicos | Terabytes/día **[estimado]** | CloudWatch + streaming **[inferencia]** | SRE, disponibilidad, seguridad operativa |

> **Nota de escala**: 4.184 millones de transacciones anuales equivalen a ~11,5 millones por día o ~133 transacciones por segundo en promedio, con picos muy superiores.

---

## 4. Arquitectura propuesta

La arquitectura sigue el patrón **Medallion (Bronze → Silver → Gold)** sobre **AWS como cloud principal** (confirmado), con **dos rieles paralelos**: uno analítico (batch + micro-batch) y uno operacional (streaming de baja latencia para fraude, pagos QR/Bre-B y autenticación biométrica). El nuevo core COREX sobre Thought Machine, al ser nativamente orientado a eventos, cambia la narrativa de ingesta respecto a un banco con core AS400 puro: los eventos fluyen en tiempo real desde el origen.

### Bronze

La capa Bronze es el lago de datos **crudo, inmutable y auditable**. Nada de negocio se corrige aquí.

- **Qué se guarda**: eventos del core COREX, payloads de APIs de pagos QR/Bre-B, logs de canales digitales, documentos de trámites, telemetría de biometría, encuestas VOCE, snapshots de CRM, datos de Open Finance.
- **Formatos**: Parquet (Snappy) para datos estructurados; JSON para eventos de APIs y apps; Avro para tópicos Kafka (esquema embebido); PDF/imágenes para documentos procesados por AIO.
- **Particionamiento**: `s3://data-lake/bronze/{dominio}/{pais}/{canal}/year={}/month={}/day={}/hour={}`.
- **Retención**: 7 años mínimo para datos transaccionales (regulatorio SFC). Logs técnicos: 90 días hot, 2 años S3 Glacier.
- **Controles**: schema validation (AWS Glue Schema Registry), checksums de integridad, clasificación de sensibilidad PII, linaje de ingesta, colas de error. Sin transformaciones de negocio.

### Silver

Silver es donde los datos adquieren significado de negocio: calidad, integración y conformación de entidades.

- **Transformaciones**: deduplicación por claves de negocio, estandarización de zonas horarias (UTC) y monedas, normalización de catálogos, enriquecimiento cruzando transacciones con contexto de cliente, reconciliación de estados de pago QR/Bre-B.
- **Dimensiones conformadas con SCD Tipo 2**: `Dim_Cliente` (360° integrado Core + CRM + canales + Open Finance + consentimientos), `Dim_Producto`, `Dim_Canal`, `Dim_Tiempo`, `Dim_Geografía`.
- **Datos sensibles**: tokenización PCI-DSS, enmascaramiento por columna, cifrado AES-256 vía KMS, control de acceso por columna con Lake Formation. Historización de consentimientos Open Finance.
- **Calidad**: AWS Deequ. Umbrales: completitud >99%, unicidad 100% en `transaction_id`, consistencia de montos entre canales y core.
- **Adicionalmente llegan a Silver**: señales de biometría, feedback VOCE y resultados de procesamiento documental AIO — impactan directamente en experiencia, riesgo y cumplimiento.

### Gold

Gold contiene **productos de datos por dominio de negocio**, no una bodega genérica. Incluye tanto tablas analíticas como serving tables de baja latencia.

- **Mart Clientes 360**: ingresos, gastos categorizados, productos activos, score churn, propensión de compra, NPS por touchpoint.
- **Mart Transacciones y Pagos**: fact_transacciones + fact_pagos_interoperables (QR/Bre-B). KPIs: volumen diario, ticket promedio, tasa rechazo, latencia de canal.
- **Mart Fraude / Riesgo**: score fraude, tipo alerta, canal, falsos positivos, pérdidas evitadas.
- **Mart Financiero Regulatorio**: cartera, mora, provisiones, solvencia, NIM, posición de tesorería.
- **Mart Experiencia Digital**: funnels de conversión, VOCE embebida, errores, NPS por journey.
- **Feature Store ML** (SageMaker Feature Store): features de fraude (velocidad transaccional, anomalías geográficas, reputación dispositivo), scoring crediticio (ratios deuda/ingreso, historial 12 meses, datos Open Finance), churn (días sin transacción, reducción de saldo), personalización (navegación, respuesta a interacciones).
- **Serving tables de baja latencia**: **DynamoDB** para scoring de fraude y recomendaciones en canal — patrón confirmado en AWS Summit Bogotá.

### Consumo

| Consumidor | Herramienta / Canal | Datos que consume |
|---|---|---|
| C-Level / Junta | Power BI (confirmado) / Tableros ejecutivos | KPIs negocio: cartera, solvencia, utilidades, NPS |
| Finanzas y Tesorería | BI + Reportes regulatorios automáticos | Mart financiero, provisiones, solvencia multipaís |
| Prevención de Fraude | Plataforma antifraude RT + DynamoDB serving | Feature Store fraude + streaming txns + biometría |
| Marketing / CRM / Producto | Salesforce + segmentación + campañas | Customer 360, propensión, VOCE, journeys |
| Ciencia de Datos / AIO | SageMaker Studio / Jupyter | Feature Store + Bronze/Silver, datasets versionados |
| App / Nequi / Canal digital | APIs REST/GraphQL + DynamoDB (<200ms) | Score fraude, recomendaciones, crédito pre-aprobado |
| Reguladores (SFC, SUGEF, SBP) | Reportes automáticos cifrados | Cartera, KYC, AML, solvencia por entidad |
| SRE / Operaciones TI | CloudWatch + Grafana | Logs, latencia APIs, disponibilidad canales |

---

## 5. Decisiones técnicas justificadas

### ¿ETL o ELT?

**ELT predominante con ETL selectivo en bordes sensibles.**

ELT conviene porque el banco necesita conservar el dato crudo para auditoría regulatoria, trazabilidad, reproceso histórico y entrenamiento de modelos ML. El nuevo core COREX (Thought Machine, orientado a eventos) hace muy costoso asumir que el modelo de negocio quedará fijo — mejor cargar primero y transformar después con dbt + Spark. AWS ya es el cloud confirmado, con la potencia de cómputo necesaria para transformar in-lake.

La excepción: **ETL o stream processing temprano** en bordes sensibles — tokenización de PII antes de Bronze, serving de fraude, normalización de consentimientos Open Finance y confirmación inmediata de pagos QR/Bre-B.

### ¿Batch o streaming?

**Mixto, con claro sesgo a streaming donde cada segundo tiene valor de negocio.**

| Proceso | Modalidad | Justificación |
|---|---|---|
| Fraude transaccional y account takeover | Streaming < 200ms | La decisión pierde valor si llega después de aprobar la transacción |
| QR, llaves, Bre-B — conciliación inmediata | Streaming / tiempo real | El comercio necesita confirmación y trazabilidad instantánea |
| Ingesta Core COREX (Thought Machine) | Eventos continuos | El nuevo core es nativamente orientado a eventos |
| Personalización / next best action | Near real-time + batch | Oferta en canal con contexto reciente; modelo recalibrado offline |
| Scoring crediticio con Open Finance | Near real-time + batch | Decisión inmediata; modelo y cartera se recalibran por lotes |
| Reporting regulatorio y cierres contables | Batch diario / intradía / mensual | Importa consistencia y conciliación, no latencia de ms |
| Automatización documental AIO | Event-driven + batch | Flujos disparados por evento; consolidación cierra por lotes |
| Entrenamiento de modelos ML | Batch periódico (semanal/mensual) | Reentrenamiento offline con datos históricos completos |

### ¿Cloud principal?

**AWS — dato público confirmado, con patrón híbrido.**

Evidencia pública directa: el Informe de Gestión 2025 calcula emisiones de nube a partir del uso real de recursos AWS; la presentación del AWS Summit Bogotá muestra una arquitectura real con **S3, Glue, DynamoDB y EKS** junto con Hadoop on-prem (señal del patrón híbrido real); vacantes recientes exigen AWS, PostgreSQL, DynamoDB y Kubernetes; el VP de Tecnología confirmó en abril 2025 que el 75% de las aplicaciones ya están en nube.

Puede existir convivencia con entornos SaaS corporativos, pero no hay evidencia pública equivalente que confirme otro cloud como plataforma principal de datos. Conclusión: **AWS-first híbrido**.

### Stack inferido

| Componente | Tecnología propuesta | Estado |
|---|---|---|
| **Core bancario** | Thought Machine Vault (COREX) | **[CONFIRMADO — Informe Gestión 2025]** |
| **Storage analítico (Lake)** | Amazon S3 + Delta Lake / Apache Iceberg | **[CONFIRMADO S3]** + [INFERENCIA] |
| **Storage transaccional** | Amazon RDS / Aurora PostgreSQL | **[CONFIRMADO — RDS]** |
| **Serving baja latencia** | Amazon DynamoDB + APIs sobre EKS | **[CONFIRMADO — AWS Summit Bogotá]** |
| **Compute (transformaciones)** | Apache Spark sobre AWS EMR | [INFERENCIA] |
| **Ingesta streaming** | Amazon Kinesis + Amazon MSK (Kafka) | [INFERENCIA — ecosistema AWS natural] |
| **Ingesta batch / CDC** | AWS Glue + AWS DMS | [INFERENCIA] |
| **Orquestación** | Apache Airflow — AWS MWAA | [INFERENCIA] |
| **Transformación modelado** | dbt Core / dbt Cloud | [INFERENCIA] |
| **Contenedores** | Amazon EKS (Kubernetes) | **[CONFIRMADO]** |
| **Catálogo y gobierno** | AWS Glue Data Catalog + Lake Formation | [INFERENCIA] |
| **Calidad de datos** | AWS Deequ | [INFERENCIA] |
| **BI / Visualización** | Power BI (principal) | **[CONFIRMADO — vacantes públicas]** |
| **ML Platform** | AIO (plataforma interna) + SageMaker | **[CONFIRMADO AIO — Informe Gestión 2025]** |
| **Feature Store** | SageMaker Feature Store | [INFERENCIA] |
| **Seguridad** | IAM + Lake Formation + KMS + Macie | [INFERENCIA — regulatoriamente obligatorio] |
| **Monitoreo** | CloudWatch + Grafana + X-Ray | [INFERENCIA] |

---

## 6. Reto técnico único

El desafío arquitectural más complejo de Bancolombia es **unificar identidad, consentimiento y riesgo en tiempo real sin romper auditabilidad regulatoria**. El banco no solo procesa más de 4.184 millones de transacciones al año — además opera canales digitales masivos, interoperabilidad con QR/llaves/Bre-B con trazabilidad inmediata, finanzas abiertas bajo autorización del cliente, biometría facial, asistentes conversacionales y un core (COREX sobre Thought Machine) que está siendo rehecho como plataforma modular y orientada a eventos. El problema no es "guardar más datos", sino **decidir con el contexto correcto en milisegundos**: si una transacción es fraude, si un pago se confirma, si un dato Open Finance puede compartirse, si una oferta es pertinente — y que todo quede trazado para auditoría en 6 jurisdicciones con marcos legales distintos.

---

## 7. Fuentes consultadas

| Fuente | URL | Qué aporta |
|---|---|---|
| Informe de Gestión Grupo Cibest 2025 | https://www.grupocibest.com/wcm/connect/bf5091ab-dd4e-44fd-9d43-7db2eb451f57/Bancolombia_Informe_de_Gestion_2025.pdf | Fuente más importante: COREX/Thought Machine, 4.184M txns/año, AIO, 9.4M clientes digitales, 23.649 empleados |
| Resultados financieros 2T25 — Grupo Cibest | https://www.bancolombia.com/acerca-de/sala-prensa/noticias/resultados-corporativos/grupo-cibest-resultados-financieros-segundo-trimestre-2025 | Fija clientes regionales (33M+), clientes locales (16.2M) y clientes digitales mensuales (9.4M) |
| Presentación AWS Summit Bogotá — DynamoDB + Bancolombia | https://d1.awsstatic.com/events/Summits/awsbogotasummit/Descubre_como_Bancolombia_utiliza_Amazon_DynamoDB_para_mejorar_la_experiencia_de_sus_clientes_DAT302.pdf | Confirma arquitectura real con S3, Glue, DynamoDB, EKS y coexistencia con Hadoop on-prem |
| Caso de éxito AWS — Bancolombia Murex | https://aws.amazon.com/es/solutions/case-studies/bancolombia/ | AWS como cloud principal, RDS, reducción costos $17K USD/mes, tiempo de cierre -31% |
| Entrevista VP Tecnología — Valora Analitik (Abril 2025) | https://www.valoraanalitik.com/bancolombia-avances-tecnologicos-prioridad/ | 75% apps en nube, 546 procesos automatizados, ahorro $26.000M en riesgos |
| Open Finance oficial | https://www.bancolombia.com/open-data/open-finance | Esquema de consentimiento y rol de datos externos en finanzas abiertas |
| QR interoperable ya está funcionando | https://soportedevs.bancolombia.com/hc/es-419/articles/21340946956820--El-QR-interoperable-ya-est%C3%A1-funcionamiento | Trazabilidad tiempo real para comercios — 400M txns QR+llaves en 2025 |
| Vacante Practicante Ciencia de Datos | https://empleo.grupobancolombia.com/bancolombia/job/Etipani-Practicante-Universitario-de-Ciencia-de-Datos-COL/1368911800/ | Confirma Power BI, ETLs, data warehouse y Python como stack analítico vigente |
| Política de tratamiento de datos personales | https://www.bancolombia.com/personas/documentos-legales/proteccion-datos/bancolombia-sa | Gobierno, privacidad, consentimientos, reserva bancaria y Open Finance |
| Tabot — Asistente virtual | https://www.bancolombia.com/centro-de-ayuda/canales/tabot | Confirma existencia y alcance del asistente conversacional basado en IA |

---

## 8. Cómo usé Claude (y los otros modelos)

El análisis final integra el trabajo de **tres modelos de IA** con roles distintos:

**Claude** actuó como consultor senior de arquitectura de datos. Realizó búsqueda web activa para obtener información pública actualizada (reportes trimestrales, casos de éxito de AWS, entrevistas al VP de Tecnología), diferenció en todo momento entre datos confirmados e inferidos, y construyó los artefactos finales: documento de análisis técnico completo, este README y el diagrama `.drawio` en el estilo visual del archivo de referencia Rappi del bootcamp.

**Gemini y ChatGPT** aportaron un análisis complementario que reveló tres datos clave: (1) **COREX sobre Thought Machine Vault** como core bancario de 4ta generación — hecho que cambia la narrativa de ingesta de "AS400 puro" a "core orientado a eventos en nube"; (2) el volumen real de **4.184 millones de transacciones monetarias anuales** del Informe de Gestión 2025; y (3) el patrón **DynamoDB + EKS** como arquitectura de serving de baja latencia, confirmado en la presentación del AWS Summit Bogotá.

**Lo que cuestioné y ajusté**: la hipótesis de Azure como plataforma analítica principal (sugerida por Gemini) no tiene soporte público equivalente al de AWS. La conclusión final — **AWS-first híbrido** — integra mejor toda la evidencia. El stack de Databricks mencionado por Gemini fue marcado como inferencia por falta de confirmación pública directa. El número de empleados (23.649 directos vs 34.182 del grupo) se clarificó entendiendo la diferencia de perímetro organizacional.
