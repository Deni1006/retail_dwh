
  
    

  create  table "dwh_raw"."public_gold"."gold_sales_trands__dbt_tmp"
  
  
    as
  
  (
    

SELECT 
  -- 🟢 УМНОЕ ПРЕОБРАЗОВАНИЕ: обрабатываем оба формата дат
  DATE_TRUNC('month', 
    CASE 
      WHEN order_date LIKE '%/%' THEN TO_DATE(order_date, 'DD/MM/YYYY')
      ELSE order_date::date
    END
  ) as sales_month,
  region,
  category,
  SUM(sales_amount) as monthly_sales,
  COUNT(DISTINCT order_id) as order_count,
  COUNT(DISTINCT customer_id) as unique_customers
FROM "dwh_raw"."public_silver"."silver_orders"
GROUP BY 
  DATE_TRUNC('month', 
    CASE 
      WHEN order_date LIKE '%/%' THEN TO_DATE(order_date, 'DD/MM/YYYY')
      ELSE order_date::date
    END
  ),
  region, 
  category
ORDER BY sales_month DESC, monthly_sales DESC
  );
  