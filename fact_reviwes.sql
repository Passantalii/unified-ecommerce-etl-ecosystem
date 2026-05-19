{{ config(materialized='table') }}

-- Fact Reviews Table

select

    pr.review_id,

    dc.customer_key,

    dp.product_key,

    cast(strftime(pr.review_date, '%Y%m%d') as integer) as review_date_key,

    pr.rating,

    pr.review_text

from {{ ref('product_reviews') }} pr

join {{ ref('dim_customer') }} dc
    on pr.customer_id = dc.customer_id

join {{ ref('dim_product') }} dp
    on pr.product_id = dp.product_id