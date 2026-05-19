{{ config(materialized='table') }}

-- Product Dimension Table

select

    -- Surrogate Key
    row_number() over(order by product_id) as product_key,

    -- Business Key
    product_id,

    -- Product Attributes
    product_name,
    category,
    brand,
    price,

    -- SCD Type 2 Fields
    current_date as effective_price_start_date,
    null as effective_price_end_date,
    true as is_current

from {{ ref('products') }}