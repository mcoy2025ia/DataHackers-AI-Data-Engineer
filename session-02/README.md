# Análisis de Ventas — Brazilian E-Commerce (Olist)

## Dataset y Motor SQL

El dataset proviene del repositorio público [Brazilian E-Commerce (Olist)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) disponible en Kaggle, y cubre transacciones de un marketplace brasileño entre septiembre de 2016 y agosto de 2018. El motor SQL utilizado es PostgreSQL 18 corriendo en local. Tres decisiones de diseño son fundamentales para interpretar correctamente los resultados: (1) **revenue se define como `order_items.price`** — el precio neto del ítem vendido, excluyendo `payment_value` de la tabla de pagos para evitar duplicación en órdenes con cuotas o múltiples métodos de pago; (2) **el filtro base es `order_status = 'delivered'`** en todos los retos, trabajando exclusivamente con transacciones completadas; y (3) **los clientes se identifican por `customer_unique_id`**, no por `customer_id`, porque Olist genera un `customer_id` nuevo por cada orden, lo que haría parecer que todos los compradores adquirieron exactamente una vez.

---

## Reto 1: Top 10 Productos por Ingresos

### Query

```sql
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
```

### Hallazgos

- **Health & Beauty domina el top con dos productos que suman $117.212 en ingresos.** El primer lugar genera $63.560 con un ticket promedio de $327 por unidad, y el segundo $53.652 con $350 promedio. Ambos combinan volumen razonable (186 y 148 órdenes) con precio unitario alto — perfil ideal de producto estrella.
- **Computers tiene el ticket promedio más alto del ranking: $1.392 por unidad**, con solo 33 órdenes y $45.949 en ingresos. Un solo quiebre de stock en este producto puede costar más de $1.300 por venta perdida; el margen de error logístico es mínimo.
- **La categoría Baby logra $38.907 con apenas 38 órdenes** (ticket promedio $1.024), lo que lo convierte en el producto de mayor valor relativo por transacción. Baja exposición de ventas pero altísimo impacto por orden — extremadamente sensible a la disponibilidad.
- **Bed & Bath Table y Furniture Decor ganan por volumen, no por precio.** El producto de bed_bath_table acumula 456 órdenes con un ticket de $88 y el de furniture_decor 425 órdenes a $71. Son motores de tráfico y frecuencia, no de margen por ítem.
- **El top 10 abarca cuatro categorías distintas**, lo que indica que el revenue no depende de un solo segmento — fortaleza estructural del catálogo.

**Recomendación:** El equipo **Comercial y Logística** debe establecer políticas de stock diferenciadas por perfil de producto: los dos productos de health_beauty y el de baby requieren disponibilidad garantizada de forma permanente dado su ticket alto combinado con volumen sostenido. Para computers (33 órdenes, $1.392 ticket), la prioridad es eliminar los tiempos de reposición — una semana de quiebre equivale a ~$10.000 en ventas no realizadas. Los productos de volumen bajo/ticket alto son candidatos ideales para alertas de stock automáticas antes de llegar a cero unidades.

---

## Reto 2: Variación de Ventas Month-over-Month

### Query

```sql
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
```

### Hallazgos

- **Noviembre 2017 fue el mes cumbre del período: $987.765 en ingresos y 7.289 órdenes.** Corresponde al Black Friday brasileño (*Sexta-feira Negra*), con un crecimiento MoM de +52.4% respecto a octubre. Es el único mes que superó los $900K en toda la serie.
- **Diciembre 2017 cayó un 26.5% inmediatamente después**, retrocediendo a $726.033. La demanda se adelantó masivamente al Black Friday y dejó diciembre deprimido — patrón clásico de canibalización post-promoción en e-commerce.
- **El negocio creció de forma sostenida a lo largo de 2017:** de $111.798 en enero hasta $987.765 en noviembre, aproximadamente 8.8× en diez meses. El único mes negativo significativo en ese período fue abril (-5.2%), una caída menor que se corrigió en mayo (+43.6%).
- **2018 muestra estabilización entre $826.000 y $977.000**, sin recuperar el pico de noviembre. Junio 2018 registra la mayor caída del año: -12.4%. En Brasil, junio marca el inicio del invierno austral y coincide con reducción del consumo discrecional.
- **Septiembre y diciembre de 2016 muestran datos atípicos** (1 orden cada uno, ingresos de $134 y $10 respectivamente): son artefactos del dataset, no comportamiento real del negocio, y deben excluirse de proyecciones.

**Recomendación:** El equipo de **Growth y Comercial** debe anticipar el Black Friday con al menos 6 semanas de antelación: negociación de stock adicional con sellers, activación de campañas de awareness en octubre, y límites de descuento que no canibalicen diciembre. Dado que junio–agosto muestra caídas recurrentes (-12.4%, -3.4%), ese trimestre es el indicado para probar campañas de reactivación de demanda (cuotas sin interés, envío gratis) que amortigüen la estacionalidad invernal. La estabilización de 2018 sugiere que el negocio está madurando y las palancas de crecimiento ya no son puramente orgánicas.

---

## Reto 3: Ranking de Clientes por Ingresos

### Query

```sql
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
```

### Hallazgos

- **El cliente #1 generó $13.440 en una sola orden**, compuesta por 8 ítems con un valor promedio de $1.680 cada uno. Es el cliente de mayor gasto absoluto del catálogo y compró una sola vez en septiembre de 2017 (Río de Janeiro).
- **9 de los 10 clientes top realizaron exactamente 1 orden.** Solo el cliente #2 tiene 2 compras. Esto confirma que los mayores gastadores del marketplace son compradores de alto ticket y baja frecuencia — no clientes recurrentes fidelizados.
- **Rio de Janeiro concentra 3 de los 10 primeros puestos** (ranks 1, 2 y 10), con tickets totales de $13.440, $7.388 y $4.400 respectivamente. Es el estado con mayor presencia entre los clientes VIP en términos de revenue individual.
- **El rango de gasto entre el rank 1 y el rank 10 es muy amplio:** de $13.440 a $4.400. El primer cliente gasta 3× más que el décimo, lo que indica una distribución muy concentrada en la cima.
- **El cliente #4 (Campo Grande, MS) pagó $6.735 en una sola transacción**, con un valor por ítem de $6.735 — un artículo de alto valor en una ciudad secundaria, con gasto equivalente al de los grandes centros urbanos.

**Recomendación:** El equipo de **CRM** debe crear un segmento VIP basado en gasto total acumulado, no en frecuencia de compra. El insight crítico es que estos clientes no repiten — pero no por mala experiencia, sino porque compraron un ítem de alta inversión (electrónico, mueble, joya) con ciclo de recompra largo. La acción concreta: activar una secuencia de retención a los 90, 180 y 365 días post-compra con categorías complementarias a lo que adquirieron (accesorios, garantías extendidas, productos relacionados). Dado el peso de RJ en este segmento, las campañas de upselling dirigidas a ese estado tendrían retorno desproporcionado.

---

## Reto 4: Participación por Categoría de Producto

### Query

```sql
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
```

### Hallazgos

- **Health & Beauty es la categoría líder con el 9.33% del revenue total ($1.233M)**, seguida de Watches & Gifts (8.82%, $1.166M) y Bed, Bath & Table (7.74%, $1.023M). Solo estas tres categorías explican el 25.89% del negocio.
- **18 categorías concentran el 80% del revenue** — principio de Pareto confirmado en el dato de `cumulative_pct`. El restante 20% está distribuido entre 56 categorías, muchas con participaciones menores al 0.5%.
- **Computers tiene solo 177 órdenes pero genera $218.684 (1.65% del total)**: implica un ticket promedio por orden de ~$1.235. Es la categoría de mayor valor unitario en el top 20 — con potencial significativo si se escala el volumen de órdenes.
- **"Unknown" aparece con $170.727 en revenue** (1.29%, 1.392 órdenes, 584 productos sin categoría asignada). Es revenue real que el negocio no puede atribuir ni optimizar por segmento.
- **Las últimas 10 categorías del ranking aportan menos del 0.1% del revenue cada una** (security_and_services: $283, fashion_childrens_clothes: $520, pc_gamer: $1.307). El costo operativo de mantener un catálogo activo para estas categorías probablemente supera el beneficio marginal.

**Recomendación:** El equipo **Comercial** debe enfocar el 80% del presupuesto de captación de sellers y campañas de marketing en las 18 categorías que explican el 80% del revenue. Concretamente, Health & Beauty, Watches & Gifts y Bed & Bath merecen managers de categoría dedicados y acuerdos de exclusividad con sellers top. Para Computers, el objetivo no es margen sino volumen — una estrategia de cuotas sin interés puede mover el ticket promedio ya existente hacia más órdenes. La categoría "Unknown" requiere una campaña de normalización de datos con los sellers: cada producto sin categoría es revenue invisible que no se puede escalar ni segmentar.

---

## Reto 5: Ticket Promedio por Estado (Análisis Libre)

### Pregunta planteada

¿En qué estados de Brasil el ticket promedio por orden es más alto, y cómo se distribuyen los estados en cuartiles de valor?

### Justificación

El ticket promedio por estado revela dónde el cliente ya tiene alta disposición de pago, independientemente del volumen de órdenes. Combinado con el desglose de flete, permite identificar si el gasto elevado responde a preferencia de producto o simplemente al costo logístico que el comprador absorbe. Es un dato directo para que los equipos de Growth y Supply Chain tomen decisiones de expansión y política de envíos con base geográfica real.

### Query

```sql
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
```

### Hallazgos

- **Paraíba (PB) lidera el ticket promedio con $266.61 por orden**, superando en 87% a São Paulo ($142.46), el estado con ticket más bajo. PB tiene 517 órdenes — volumen modesto pero clientes de alto valor unitario comprobado.
- **Los 7 estados del Q1 (alto valor) son todos del Norte o Nordeste:** PB, AC, AP, AL, RO, PA, PI. Son estados remotos donde el flete promedio oscila entre $38 y $49, versus $17 en São Paulo. El cliente del interior compra menos veces pero gasta más por orden, probablemente consolidando múltiples necesidades en una sola compra.
- **São Paulo tiene 40.501 órdenes — 78× más que Amapá (67 órdenes)** — pero el ticket más bajo ($142.46) y el flete más barato ($17.33). Genera $5.769.703 en revenue total, la mayor contribución absoluta por lejos. Es el motor de volumen del negocio, no de ticket.
- **Acre (AC) y Amapá (AP) tienen tickets de $244 y $240 respectivamente, con apenas 80 y 67 órdenes.** Son mercados desatendidos con alta disposición de pago demostrada: si se reduce la fricción de entrega, el potencial de crecimiento es considerable.
- **El flete en estados del Norte/Nordeste representa entre el 19% y 22% del ticket total** (ej. RO: $46.43 flete sobre $234.43 ticket = 19.8%), versus apenas el 12% en SP ($17.33 sobre $142.46). El costo logístico es una barrera de conversión diferenciada por región.

**Recomendación:** El equipo de **Logística y Growth** debe diseñar una estrategia geográfica en dos velocidades: (1) para los estados Q1 del Norte/Nordeste (PB, AC, AP, AL, RO), implementar subsidio de envío o umbrales de envío gratis más bajos — dado que el ticket ya es alto, absorber parte del flete tiene ROI positivo en conversión; (2) para São Paulo y los estados Q4, la palanca no es el flete sino la frecuencia — programas de suscripción, cashback o recompra automática dirigidos al segmento más activo. Acre y Amapá merecen un piloto de expansión logística: el cliente ya paga precios altos, solo falta reducir el tiempo de entrega para destrabar demanda latente.

---

## Conclusión general

El análisis del dataset Olist 2016–2018 revela un negocio en fase de crecimiento acelerado con señales claras de maduración hacia el final del período. El motor del revenue es dual: categorías de alto ticket y frecuencia media (Health & Beauty, Watches & Gifts) coexisten con productos de volumen masivo y ticket bajo (Bed & Bath, Furniture), lo que confiere estabilidad estructural al catálogo. La concentración geográfica en São Paulo es el mayor activo en términos de volumen, pero los estados del Norte y Nordeste esconden la mayor disposición de pago por orden — una oportunidad logística no explotada. El perfil de los clientes top (compradores únicos de alto ticket) señala que el desafío central no es adquisición sino retención: convertir una compra de $13.000 en una relación de largo plazo requiere CRM activo, no solo un buen producto. Finalmente, el pico de noviembre 2017 confirma que el Black Friday es el evento comercial más relevante del año y debe planificarse con meses de anticipación para maximizar el impacto sin canibalizar diciembre.
