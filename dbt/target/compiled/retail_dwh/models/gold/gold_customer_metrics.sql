

SELECT 
  customer_id,
  customer_name,
  segment,
  country,
  city,
  region,
  COUNT(DISTINCT order_id) as total_orders,
  SUM(sales_amount) as total_sales,
  AVG(sales_amount) as avg_order_value,
  MAX(order_date) as last_order_date
FROM "dwh_raw"."public_silver"."silver_orders"
GROUP BY customer_id, customer_name, segment, country, city, region