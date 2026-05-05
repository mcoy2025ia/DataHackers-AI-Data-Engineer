-- Setup script: creates all Olist tables and loads CSV data
-- Run against olist_db after database creation

-- ── Customers ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS olist_customers_dataset (
    customer_id              VARCHAR(50),
    customer_unique_id       VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city            VARCHAR(100),
    customer_state           VARCHAR(5)
);

-- ── Orders ───────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS olist_orders_dataset (
    order_id                        VARCHAR(50),
    customer_id                     VARCHAR(50),
    order_status                    VARCHAR(50),
    order_purchase_timestamp        TIMESTAMP,
    order_approved_at               TIMESTAMP,
    order_delivered_carrier_date    TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    order_estimated_delivery_date   TIMESTAMP
);

-- ── Order Items ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS olist_order_items_dataset (
    order_id             VARCHAR(50),
    order_item_id        INTEGER,
    product_id           VARCHAR(50),
    seller_id            VARCHAR(50),
    shipping_limit_date  TIMESTAMP,
    price                NUMERIC(10,2),
    freight_value        NUMERIC(10,2)
);

-- ── Order Payments ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS olist_order_payments_dataset (
    order_id              VARCHAR(50),
    payment_sequential    INTEGER,
    payment_type          VARCHAR(50),
    payment_installments  INTEGER,
    payment_value         NUMERIC(10,2)
);

-- ── Products ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS olist_products_dataset (
    product_id                   VARCHAR(50),
    product_category_name        VARCHAR(100),
    product_name_lenght          INTEGER,
    product_description_lenght   INTEGER,
    product_photos_qty           INTEGER,
    product_weight_g             NUMERIC(10,2),
    product_length_cm            NUMERIC(10,2),
    product_height_cm            NUMERIC(10,2),
    product_width_cm             NUMERIC(10,2)
);

-- ── Sellers ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS olist_sellers_dataset (
    seller_id               VARCHAR(50),
    seller_zip_code_prefix  VARCHAR(10),
    seller_city             VARCHAR(100),
    seller_state            VARCHAR(5)
);

-- ── Category Translation ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS product_category_name_translation (
    product_category_name          VARCHAR(100),
    product_category_name_english  VARCHAR(100)
);

-- ── Geolocation (extra, not in CLAUDE.md but present in data/) ───────────────
CREATE TABLE IF NOT EXISTS olist_geolocation_dataset (
    geolocation_zip_code_prefix  VARCHAR(10),
    geolocation_lat              DOUBLE PRECISION,
    geolocation_lng              DOUBLE PRECISION,
    geolocation_city             VARCHAR(100),
    geolocation_state            VARCHAR(5)
);

-- ── Order Reviews (extra, not in CLAUDE.md but present in data/) ─────────────
CREATE TABLE IF NOT EXISTS olist_order_reviews_dataset (
    review_id                VARCHAR(50),
    order_id                 VARCHAR(50),
    review_score             INTEGER,
    review_comment_title     VARCHAR(500),
    review_comment_message   TEXT,
    review_creation_date     TIMESTAMP,
    review_answer_timestamp  TIMESTAMP
);
