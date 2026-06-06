-- Assessment Q1: Which color generated the highest revenue each year?

DROP TABLE IF EXISTS analysis_revenue_by_color_year;

CREATE TABLE analysis_revenue_by_color_year AS
WITH revenue_per_color_year AS (
    -- Sum TotalLineExtendedPrice grouped by year and product color
    SELECT
        EXTRACT(YEAR FROM o.order_date)::INTEGER          AS year,
        p.color,
        SUM(o.total_line_extended_price)                  AS total_revenue
    FROM publish_orders o
    JOIN publish_product p USING (product_id)
    GROUP BY 1, 2
),
ranked AS (
    -- Rank colors within each year by total revenue (highest = rank 1)
    SELECT
        year,
        color,
        ROUND(total_revenue, 2) AS total_revenue,
        RANK() OVER (PARTITION BY year ORDER BY total_revenue DESC) AS revenue_rank
    FROM revenue_per_color_year
)
SELECT year, color, total_revenue
FROM ranked
WHERE revenue_rank = 1
ORDER BY year;
