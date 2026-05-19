select 
    order_item_id,            -- Keep order_item_id as is
    order_id,                 -- Keep order_id as is
    product_id,               -- Keep product_id as is
    quantity as order_quantity,    -- Rename user_id to customer_id
    unit_price,               -- Keep unit_price as is
from {{ ref('order_items') }}   -- Reference the raw_order_items model
where order_item_id is not null           -- Filter out rows where id is null