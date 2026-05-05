WITH customer_revenue AS (
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
