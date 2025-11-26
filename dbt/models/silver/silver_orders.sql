{{ config(tags=["silver"]) }}

WITH ranked_orders AS (
  SELECT 
    "Row_ID" as row_id,
    "Order_ID" as order_id,
    -- 🟢 УПРОЩАЕМ: Оставляем даты как есть (text), преобразуем позже
    "Order_Date" as order_date,
    "Ship_Date" as ship_date,
    "Ship_Mode" as ship_mode,
    "Customer_ID" as customer_id,
    "Customer_Name" as customer_name,
    "Segment" as segment,
    "Country" as country,
    "City" as city,
    "State" as state,
    "Postal_Code" as postal_code,
    "Region" as region,
    "Product_ID" as product_id,
    "Category" as category,
    "Sub_Category" as sub_category,
    "Product_Name" as product_name,
    "Sales"::decimal as sales_amount,
    ROW_NUMBER() OVER(PARTITION BY "Order_ID" ORDER BY "Order_Date" DESC) as rn
  FROM {{ source('raw', 'superstore_raw') }}
  WHERE "Order_ID" IS NOT NULL
)
SELECT 
  row_id,
  order_id,
  order_date,
  ship_date,
  ship_mode,
  customer_id,
  customer_name,
  segment,
  country,
  city,
  state,
  postal_code,
  region,
  product_id,
  category,
  sub_category,
  product_name,
  sales_amount
FROM ranked_orders 
WHERE rn = 1