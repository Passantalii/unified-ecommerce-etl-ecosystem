{{ config(materialized='table') }}

-- Fact Sales Table

select

    oi.order_item_id,

    dc.customer_key,

    dp.product_key,

    cast(strftime(o.order_date, '%Y%m%d') as integer) as order_date_key,

    oi.order_id,

    oi.quantity,

    oi.unit_price,

    (oi.quantity * oi.unit_price) as total_amount,

    o.payment_method

from {{ ref('order_items') }} oi

join {{ ref('orders') }} o
    on oi.order_id = o.order_id

join {{ ref('dim_customer') }} dc
    on o.customer_id = dc.customer_id

join {{ ref('dim_product') }} dp
    on oi.product_id = dp.product_id
