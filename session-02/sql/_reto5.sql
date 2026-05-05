WITH order_totals AS (
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
    SELECT
        customer_state,
        total_orders,
        avg_ticket,
        avg_items_value,
        avg_freight,
        total_revenue,
        RANK() OVER (ORDER BY avg_ticket DESC)       AS ticket_rank,
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
    CASE ticket_quartile
        WHEN 1 THEN 'Q1 - Alto valor'
        WHEN 2 THEN 'Q2 - Valor medio-alto'
        WHEN 3 THEN 'Q3 - Valor medio-bajo'
        WHEN 4 THEN 'Q4 - Bajo valor'
    END AS value_segment
FROM state_ranked
ORDER BY ticket_rank;
