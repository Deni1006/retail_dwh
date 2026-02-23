{{ config(materialized='table') }}

with src as (

    select
        -- ключ строки (лучший вариант для дедупа в Superstore)
        nullif(trim(row_id), '')::int as row_id,

        nullif(trim(order_id), '') as order_id,

        -- даты: ловим самый частый формат D/M/YYYY или DD/MM/YYYY
        case
            when order_date ~ '^\s*\d{1,2}/\d{1,2}/\d{4}\s*$'
                then to_date(trim(order_date), 'FMDD/FMMM/YYYY')
            when order_date ~ '^\s*\d{4}-\d{2}-\d{2}\s*$'
                then trim(order_date)::date
            else null
        end as order_date,

        case
            when ship_date ~ '^\s*\d{1,2}/\d{1,2}/\d{4}\s*$'
                then to_date(trim(ship_date), 'FMDD/FMMM/YYYY')
            when ship_date ~ '^\s*\d{4}-\d{2}-\d{2}\s*$'
                then trim(ship_date)::date
            else null
        end as ship_date,

        nullif(trim(ship_mode), '') as ship_mode,

        nullif(trim(customer_id), '') as customer_id,
        nullif(trim(customer_name), '') as customer_name,
        nullif(trim(segment), '') as segment,

        nullif(trim(country), '') as country,
        nullif(trim(city), '') as city,
        nullif(trim(state), '') as state,
        nullif(trim(postal_code), '') as postal_code,
        nullif(trim(region), '') as region,

        nullif(trim(product_id), '') as product_id,
        nullif(trim(category), '') as category,
        nullif(trim(sub_category), '') as sub_category,
        nullif(trim(product_name), '') as product_name,

        -- выручка по позиции заказа
        nullif(trim(sales), '')::numeric(18, 4) as sales_amount

    from {{ source('raw', 'superstore_raw') }}

    where nullif(trim(order_id), '') is not null

),

dedup as (

    -- Дедуп: оставляем одну запись на row_id (если вдруг дубль строки)
    select
        *,
        row_number() over (
            partition by row_id
            order by order_date desc nulls last
        ) as rn
    from src

)

select
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
from dedup
where rn = 1