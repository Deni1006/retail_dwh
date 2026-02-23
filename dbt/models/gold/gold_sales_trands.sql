{{ config(tags=["gold"]) }}

select
  date_trunc('month', order_date)::date as sales_month,
  region,
  category,
  sum(sales_amount) as monthly_sales,
  count(distinct order_id) as order_count,
  count(distinct customer_id) as unique_customers
from {{ ref('silver_orders') }}
where order_date is not null
group by
  1, 2, 3
order by sales_month desc, monthly_sales desc