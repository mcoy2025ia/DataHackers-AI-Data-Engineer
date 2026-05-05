SELECT
    oi.product_id,
    COALESCE(ct.product_category_name_english, p.product_category_name, 'Unknown') AS category,
    COUNT(DISTINCT o.order_id)          AS total_orders,
    SUM(oi.price)                       AS total_revenue,
    ROUND(AVG(oi.price)::NUMERIC, 2)    AS avg_price_per_unit
FROM olist_orders_dataset           o
JOIN olist_order_items_dataset      oi  ON o.order_id       = oi.order_id
JOIN olist_products_dataset         p   ON oi.product_id    = p.product_id
LEFT JOIN product_category_name_translation ct
                                        ON p.product_category_name = ct.product_category_name
WHERE o.order_status = 'delivered'
GROUP BY
    oi.product_id,
    p.product_category_name,
    ct.product_category_name_english
ORDER BY total_revenue DESC
LIMIT 10;
