-- ============================================================
--  MINI-PROYECTO: Análisis de Ventas con SQL Avanzado
--  Dataset  : Brazilian E-Commerce (Olist) — Kaggle
--  Motor    : PostgreSQL
--  Revenue  : order_items.price  (precio neto del ítem)
--  Filtro   : Solo órdenes con status = 'delivered'
--  Autor    : Manuel Alberto Coy Benavides
-- ============================================================
-- Tablas usadas (nombres estándar de Olist al importar los CSVs):
--   olist_orders_dataset                  → orders
--   olist_order_items_dataset             → order_items
--   olist_order_payments_dataset          → payments
--   olist_customers_dataset               → customers
--   olist_products_dataset                → products
--   product_category_name_translation     → category_translation
-- ============================================================


-- ============================================================
-- RETO 1: Top 10 Productos por Ingresos
-- Objetivo : Identificar los 10 productos que más revenue
--            generan para priorizar decisiones de inventario.
-- Conceptos: JOIN, GROUP BY, ORDER BY, SUM
-- Nota     : Olist no tiene nombre de producto individual;
--            agrupamos por product_id y mostramos la categoría
--            traducida al inglés como referencia comercial.
-- ============================================================

SELECT
    oi.product_id,
    -- Categoría en inglés para comunicación con el equipo
    COALESCE(ct.product_category_name_english, p.product_category_name, 'Unknown') AS category,
    COUNT(DISTINCT o.order_id)          AS total_orders,
    SUM(oi.price)                       AS total_revenue,
    ROUND(AVG(oi.price)::NUMERIC, 2)    AS avg_price_per_unit
FROM olist_orders_dataset           o
JOIN olist_order_items_dataset      oi  ON o.order_id       = oi.order_id
JOIN olist_products_dataset         p   ON oi.product_id    = p.product_id
LEFT JOIN product_category_name_translation ct
                                        ON p.product_category_name = ct.product_category_name
-- Solo órdenes efectivamente entregadas
WHERE o.order_status = 'delivered'
GROUP BY
    oi.product_id,
    p.product_category_name,
    ct.product_category_name_english
ORDER BY total_revenue DESC
LIMIT 10;

/*
INTERPRETACIÓN (completar con resultados reales):
- Los X primeros productos pertenecen a la categoría Y, lo que confirma
  que es el segmento de mayor rotación y valor para el negocio.
- El ticket promedio por unidad varía entre $A y $B, indicando que el
  revenue proviene más de volumen que de precio unitario.
- Recomendación: garantizar stock prioritario para estos product_ids
  en los centros de distribución con mayor demanda.
*/


-- ============================================================
-- RETO 2: Variación de Ventas Month-over-Month (MoM)
-- Objetivo : Calcular ventas totales por mes y la variación %
--            respecto al mes anterior para detectar tendencias.
-- Conceptos: DATE_TRUNC, LAG(), window functions, cálculo %
-- Nota     : Excluimos el último mes del dataset si está
--            incompleto (< 15 días de datos) para evitar
--            caídas artificiales al final de la serie.
-- ============================================================

WITH monthly_sales AS (
    -- Paso 1: Agregar ventas por mes calendario
    SELECT
        DATE_TRUNC('month', o.order_purchase_timestamp)::DATE  AS sales_month,
        SUM(oi.price)                                           AS total_revenue,
        COUNT(DISTINCT o.order_id)                              AS total_orders
    FROM olist_orders_dataset       o
    JOIN olist_order_items_dataset  oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY DATE_TRUNC('month', o.order_purchase_timestamp)
),
mom_calc AS (
    -- Paso 2: Calcular variación % usando LAG para traer el mes anterior
    SELECT
        sales_month,
        total_revenue,
        total_orders,
        LAG(total_revenue) OVER (ORDER BY sales_month)  AS prev_month_revenue,
        ROUND(
            (
                (total_revenue - LAG(total_revenue) OVER (ORDER BY sales_month))
                / NULLIF(LAG(total_revenue) OVER (ORDER BY sales_month), 0)
            ) * 100
        , 2)                                             AS mom_pct_change
    FROM monthly_sales
)
SELECT
    TO_CHAR(sales_month, 'YYYY-MM')     AS month,
    ROUND(total_revenue::NUMERIC, 2)    AS total_revenue,
    total_orders,
    ROUND(prev_month_revenue::NUMERIC, 2) AS prev_month_revenue,
    mom_pct_change,
    -- Etiqueta semáforo para lectura rápida en reportes
    CASE
        WHEN mom_pct_change >  10 THEN '🟢 Crecimiento fuerte'
        WHEN mom_pct_change >= 0  THEN '🟡 Crecimiento leve'
        WHEN mom_pct_change >= -10 THEN '🟠 Caída leve'
        ELSE                           '🔴 Caída fuerte'
    END AS trend_label
FROM mom_calc
ORDER BY sales_month;

/*
INTERPRETACIÓN (completar con resultados reales):
- Mes con mayor crecimiento MoM: [MES] con +X% respecto al anterior.
  Probable causa: temporada alta / campaña promocional.
- Mes con mayor caída MoM: [MES] con -Y%. Investigar si coincide con
  problemas logísticos, estacionalidad o fin de dataset incompleto.
- La tendencia general muestra [crecimiento / estabilidad / declive]
  a lo largo del período analizado.
*/


-- ============================================================
-- RETO 3: Ranking de Clientes por Ingresos
-- Objetivo : Identificar los clientes más valiosos por gasto
--            total acumulado para acciones de fidelización.
-- Conceptos: DENSE_RANK(), OVER(ORDER BY), GROUP BY
-- Nota CRÍTICA: Olist asigna un customer_id distinto por orden.
--   Usamos customer_unique_id para identificar al cliente real
--   (mismo comprador en múltiples órdenes).
-- ============================================================

WITH customer_revenue AS (
    -- Paso 1: Consolidar revenue por cliente único
    SELECT
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        COUNT(DISTINCT o.order_id)          AS total_orders,
        SUM(oi.price)                       AS total_revenue,
        ROUND(AVG(oi.price)::NUMERIC, 2)    AS avg_order_value,
        MIN(o.order_purchase_timestamp)     AS first_purchase,
        MAX(o.order_purchase_timestamp)     AS last_purchase
    FROM olist_orders_dataset           o
    JOIN olist_order_items_dataset      oi  ON o.order_id       = oi.order_id
    JOIN olist_customers_dataset        c   ON o.customer_id    = c.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY
        c.customer_unique_id,
        c.customer_city,
        c.customer_state
)
SELECT
    -- Ranking por revenue total descendente
    DENSE_RANK() OVER (ORDER BY total_revenue DESC)  AS revenue_rank,
    customer_unique_id,
    customer_city,
    customer_state,
    total_orders,
    ROUND(total_revenue::NUMERIC, 2)    AS total_revenue,
    avg_order_value,
    TO_CHAR(first_purchase, 'YYYY-MM-DD') AS first_purchase,
    TO_CHAR(last_purchase,  'YYYY-MM-DD') AS last_purchase
FROM customer_revenue
ORDER BY revenue_rank
LIMIT 10;

/*
INTERPRETACIÓN (completar con resultados reales):
- El top 10 de clientes concentra aprox. X% del revenue total,
  lo que indica [alta / baja] concentración de valor en pocos clientes.
- La mayoría de los top clientes provienen de [estado/ciudad], lo que
  sugiere focalizar programas de lealtad en esa región.
- Acción de negocio sugerida: crear un segmento VIP con estos
  customer_unique_id, ofrecerles envío prioritario o descuentos
  exclusivos en sus categorías históricas de compra.
*/


-- ============================================================
-- RETO 4: Participación por Categoría de Producto
-- Objetivo : Calcular qué % del revenue total representa cada
--            categoría para identificar el core del negocio.
-- Conceptos: SUM() OVER () sin partición, división, formateo %
-- ============================================================

WITH category_revenue AS (
    -- Paso 1: Revenue por categoría (traducida al inglés)
    SELECT
        COALESCE(ct.product_category_name_english,
                 p.product_category_name,
                 'Unknown')          AS category,
        SUM(oi.price)                AS category_revenue,
        COUNT(DISTINCT oi.product_id) AS unique_products,
        COUNT(DISTINCT o.order_id)   AS total_orders
    FROM olist_orders_dataset               o
    JOIN olist_order_items_dataset          oi  ON o.order_id    = oi.order_id
    JOIN olist_products_dataset             p   ON oi.product_id = p.product_id
    LEFT JOIN product_category_name_translation ct
                                                ON p.product_category_name = ct.product_category_name
    WHERE o.order_status = 'delivered'
    GROUP BY
        ct.product_category_name_english,
        p.product_category_name
)
SELECT
    category,
    ROUND(category_revenue::NUMERIC, 2)  AS category_revenue,
    total_orders,
    unique_products,
    -- SUM() OVER () sin partición = gran total de todas las categorías
    ROUND(
        (category_revenue / SUM(category_revenue) OVER ()) * 100
    , 2)                                 AS revenue_share_pct,
    -- Revenue acumulado para análisis Pareto
    ROUND(
        SUM(category_revenue) OVER (ORDER BY category_revenue DESC)
        / SUM(category_revenue) OVER () * 100
    , 2)                                 AS cumulative_pct
FROM category_revenue
ORDER BY category_revenue DESC;

/*
INTERPRETACIÓN (completar con resultados reales):
- La categoría dominante es [X] con Y% del revenue total. Es el pilar
  del negocio y no debe tener quiebres de inventario bajo ninguna circunstancia.
- Las primeras N categorías explican el 80% del revenue (curva de Pareto),
  lo que orienta dónde concentrar esfuerzo comercial y logístico.
- Mayor potencial de crecimiento: [categoría] tiene alta cantidad de
  órdenes pero ticket promedio bajo — margen para upselling o bundle.
*/


-- ============================================================
-- RETO 5: Ticket Promedio por Estado (Región Geográfica)
-- Pregunta : ¿En qué estados de Brasil el ticket promedio por
--            orden es más alto, y cómo se distribuyen los estados
--            en cuartiles de valor?
-- Justificación: Permite focalizar campañas de upselling y
--   decisiones de expansión logística donde el cliente ya tiene
--   mayor disposición de pago. Dato directo para Growth y Supply.
-- Conceptos: CTE, AVG(), SUM(), NTILE(4), OVER()
-- ============================================================

WITH order_totals AS (
    -- Paso 1: Calcular el valor total de cada orden (precio + flete)
    --         El flete es parte del gasto real del cliente y es
    --         relevante geográficamente: estados lejanos pagan más.
    SELECT
        o.order_id,
        c.customer_state,
        c.customer_city,
        SUM(oi.price)          AS items_revenue,
        SUM(oi.freight_value)  AS freight_cost,
        SUM(oi.price + oi.freight_value) AS order_total
    FROM olist_orders_dataset           o
    JOIN olist_order_items_dataset      oi  ON o.order_id    = oi.order_id
    JOIN olist_customers_dataset        c   ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY
        o.order_id,
        c.customer_state,
        c.customer_city
),
state_metrics AS (
    -- Paso 2: Agregar métricas por estado
    SELECT
        customer_state,
        COUNT(DISTINCT order_id)            AS total_orders,
        ROUND(AVG(order_total)::NUMERIC, 2) AS avg_ticket,
        ROUND(AVG(items_revenue)::NUMERIC, 2) AS avg_items_value,
        ROUND(AVG(freight_cost)::NUMERIC, 2)  AS avg_freight,
        ROUND(SUM(order_total)::NUMERIC, 2)   AS total_revenue
    FROM order_totals
    GROUP BY customer_state
),
state_ranked AS (
    -- Paso 3: Ranking y cuartiles de ticket promedio
    SELECT
        customer_state,
        total_orders,
        avg_ticket,
        avg_items_value,
        avg_freight,
        total_revenue,
        -- Ranking absoluto por ticket promedio
        RANK() OVER (ORDER BY avg_ticket DESC)       AS ticket_rank,
        -- Cuartil: Q1 = estados con ticket más alto, Q4 = más bajo
        NTILE(4) OVER (ORDER BY avg_ticket DESC)     AS ticket_quartile
    FROM state_metrics
)
SELECT
    ticket_rank,
    customer_state,
    total_orders,
    avg_ticket,
    avg_items_value,
    avg_freight,
    total_revenue,
    -- Etiqueta del cuartil para legibilidad ejecutiva
    CASE ticket_quartile
        WHEN 1 THEN 'Q1 — Alto valor'
        WHEN 2 THEN 'Q2 — Valor medio-alto'
        WHEN 3 THEN 'Q3 — Valor medio-bajo'
        WHEN 4 THEN 'Q4 — Bajo valor'
    END AS value_segment
FROM state_ranked
ORDER BY ticket_rank;

/*
INTERPRETACIÓN (completar con resultados reales):
- Los estados del Q1 (alto valor) son: [lista]. Son los mercados donde
  el cliente promedio gasta más por orden — ideales para campañas de
  productos premium y mayor inversión en experiencia de entrega.
- [Estado X] llama la atención: alto ticket pero bajo volumen de órdenes,
  lo que indica mercado desatendido con potencial de crecimiento.
- El flete promedio en estados lejanos como [AC, RO, AM] es
  significativamente mayor — considerar subsidio de envío como
  palanca de conversión en esos mercados.
- Acción recomendada: segmentar la base de clientes por estado +
  cuartil de ticket para personalizar ofertas y umbrales de envío gratis.
*/
