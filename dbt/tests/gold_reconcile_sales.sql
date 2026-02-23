with silver as (
    select
        coalesce(sum(sales_amount), 0) as silver_sales
    from {{ ref('silver_orders') }}
),
gold as (
    select
        coalesce(sum(monthly_sales), 0) as gold_sales
    from {{ ref('gold_sales_trands') }}
),
diff as (
    select
        silver.silver_sales,
        gold.gold_sales,
        abs(silver.silver_sales - gold.gold_sales) as abs_diff
    from silver
    cross join gold
)
select
    silver_sales,
    gold_sales,
    abs_diff
from diff
where abs_diff > 0.01