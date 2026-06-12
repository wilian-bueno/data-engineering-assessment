-- Typed version of raw_sales_order_detail.
-- Two physical FKs enforced:
--   · sales_order_id → store_sales_order_header  (header must be loaded first)
--   · product_id     → store_product_master       (products must be loaded first)

CREATE TABLE IF NOT EXISTS store_sales_order_detail (
    sales_order_detail_id  INTEGER                  NOT NULL PRIMARY KEY,
    sales_order_id         INTEGER                  NOT NULL
                               REFERENCES store_sales_order_header (sales_order_id),
    order_qty              INTEGER,
    product_id             INTEGER
                               REFERENCES store_product_master (product_id),
    unit_price             DECIMAL(18,6),
    unit_price_discount    DECIMAL(18,6),
    etl_created_at         TIMESTAMP WITH TIME ZONE,
    source_file_name       TEXT
);
