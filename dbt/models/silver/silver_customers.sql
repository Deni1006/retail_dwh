{{ config(tags=["silver"]) }}

select
  customer_id,
  max(customer_name) as customer_name,
  max(segment) as segment,
  max(country) as country,
  max(city) as city,
  max(state) as state,
  max(postal_code) as postal_code,
  max(region) as region
from {{ ref('silver_orders') }}
where customer_id is not null
group by customer_id