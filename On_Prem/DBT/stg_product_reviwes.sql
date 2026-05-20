select
    review_id,             -- Keep review_id as is
    product_id,            -- Keep product_id as is
    customer_id,           -- Keep customer_id as is
    rating,                -- Keep rating as is
    review_text,            -- Keep review_text as is
    review_date             -- Keep review_date as is
from {{ ref('product_reviews') }}   -- Reference the product_reviews model
where review_id is not null   -- Filter out rows where review_id is null
