-- Typed version of raw_sales_order_header.

CREATE TABLE IF NOT EXISTS store_sales_order_header (
    sales_order_id     INTEGER          NOT NULL PRIMARY KEY,
    order_date         DATE             NOT NULL,
    ship_date          DATE             NOT NULL,
    online_order_flag  BOOLEAN,
    account_number     VARCHAR(20),
    customer_id        INTEGER          NOT NULL,
    sales_person_id    INTEGER,
    freight            DECIMAL(18,4),
    etl_created_at     TIMESTAMP WITH TIME ZONE,
    source_file_name   TEXT
);
