

SELECT 
  "Customer_ID" as customer_id,
  "Customer_Name" as customer_name,
  "Segment" as segment,
  "Country" as country,
  "City" as city, 
  "State" as state,
  "Postal_Code" as postal_code,
  "Region" as region
FROM "dwh_raw"."raw"."superstore_raw"
WHERE "Customer_ID" IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8