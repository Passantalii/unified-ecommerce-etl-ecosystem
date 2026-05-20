select
    order_id,            -- keep order_id as is
    customer_id,         -- keep customer_id as is
    order_date,         -- keep order_date as is
    total_amount,       -- keep total_amount as is
    payment_method,     -- keep payment_method as is
    shipping_country,     -- keep shipping_country as is
    from {{ ref('orders') }}   -- Reference the orders model
where order_id is not null   -- Filter out rows where order_id is null
