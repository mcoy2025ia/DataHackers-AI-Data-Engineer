WITH monthly_sales AS (
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
    CASE
        WHEN mom_pct_change >  10 THEN 'Crecimiento fuerte'
        WHEN mom_pct_change >= 0  THEN 'Crecimiento leve'
        WHEN mom_pct_change >= -10 THEN 'Caida leve'
        ELSE                           'Caida fuerte'
    END AS trend_label
FROM mom_calc
ORDER BY sales_month;
