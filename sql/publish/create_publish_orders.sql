-- Final business-ready orders table.

CREATE TABLE IF NOT EXISTS publish_orders (
    sales_order_detail_id         INTEGER        NOT NULL PRIMARY KEY,
    sales_order_id                INTEGER        NOT NULL,
    order_qty                     INTEGER,
    product_id                    INTEGER,
    unit_price                    DECIMAL(18,6),
    unit_price_discount           DECIMAL(18,6),
    total_line_extended_price     DECIMAL(18,6),
    order_date                    DATE,
    ship_date                     DATE,
    online_order_flag             BOOLEAN,
    account_number                VARCHAR(20),
    customer_id                   INTEGER,
    sales_person_id               INTEGER,
    total_order_freight           DECIMAL(18,4),
    lead_time_in_business_days    INTEGER,
    etl_created_at                TIMESTAMP WITH TIME ZONE
);
