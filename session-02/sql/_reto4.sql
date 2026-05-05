WITH category_revenue AS (
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
    ROUND(
        (category_revenue / SUM(category_revenue) OVER ()) * 100
    , 2)                                 AS revenue_share_pct,
    ROUND(
        SUM(category_revenue) OVER (ORDER BY category_revenue DESC)
        / SUM(category_revenue) OVER () * 100
    , 2)                                 AS cumulative_pct
FROM category_revenue
ORDER BY category_revenue DESC;
