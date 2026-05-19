select
    product_id,            -- Keep product_id as is
    product_name,          -- Keep product_name as is
    category,              -- Keep category as is
    price,                 -- Keep price as is
    stock_quantity,         -- Keep stock_quantity as is
    brand as product_brand                   -- rename it to product_brand
from {{ ref('products') }}   -- Reference the products model
where product_id is not null   -- Filter out rows where product_id is null