select
    customer_id,
    avg_order_value,
    last_order_date
from {{ ref('gold_customer_metrics') }}
where
    customer_id is null
    or avg_order_value < 0
    or last_order_date > current_date