CREATE SCHEMA IF NOT EXISTS bigbasket

CREATE TABLE IF NOT EXISTS bigbasket.products (
    product_id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    brand TEXT,
    product_url TEXT,
    images TEXT[],
    unit TEXT,
    quantity_label TEXT,

    price_mrp FLOAT,
    price_sp FLOAT,
    discount_percent FLOAT,

    is_best_value BOOLEAN,
    available_quantity INTEGER,
    availability_code TEXT,

    category_main TEXT,
    category_mid TEXT,
    category_leaf TEXT,

    created_at_on_web_site TIMESTAMP,
    updated_at_on_web_site TIMESTAMP,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);
