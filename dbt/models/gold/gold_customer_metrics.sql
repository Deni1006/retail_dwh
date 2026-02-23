{{ config(tags=["gold"]) }}

with order_totals as (
  select
    order_id,
    customer_id,
    max(order_date) as order_date,
    sum(sales_amount) as order_sales
  from {{ ref('silver_orders') }}
  where customer_id is not null
    and order_id is not null
  group by 1, 2
),

customer_agg as (
  select
    customer_id,
    count(distinct order_id) as total_orders,
    sum(order_sales) as total_sales,
    avg(order_sales) as avg_order_value,
    max(order_date) as last_order_date
  from order_totals
  group by customer_id
),

joined as (
  select
    a.customer_id,
    c.customer_name,
    c.segment,
    c.country,
    c.city,
    c.region,
    a.total_orders,
    a.total_sales,
    a.avg_order_value,
    a.last_order_date
  from customer_agg a
  left join {{ ref('silver_customers') }} c
    on a.customer_id = c.customer_id
)

select
  customer_id,
  max(customer_name) as customer_name,
  max(segment) as segment,
  max(country) as country,
  max(city) as city,
  max(region) as region,
  max(total_orders) as total_orders,
  max(total_sales) as total_sales,
  max(avg_order_value) as avg_order_value,
  max(last_order_date) as last_order_date
from joined
group by customer_id