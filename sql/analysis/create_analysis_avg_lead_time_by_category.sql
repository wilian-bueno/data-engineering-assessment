-- Assessment Q2: What is the average LeadTimeInBusinessDays by ProductCategoryName?

DROP TABLE IF EXISTS analysis_avg_lead_time_by_category;

CREATE TABLE analysis_avg_lead_time_by_category AS
SELECT
    p.product_category_name,
    ROUND(AVG(o.lead_time_in_business_days), 2)  AS avg_lead_time_business_days
FROM publish_orders o
JOIN publish_product p USING (product_id)
WHERE p.product_category_name IS NOT NULL   -- exclude products with unknown category
GROUP BY p.product_category_name
ORDER BY avg_lead_time_business_days;
