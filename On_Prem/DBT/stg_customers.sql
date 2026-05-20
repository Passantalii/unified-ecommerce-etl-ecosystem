-- Select customer-related columns from the source table
select 
    customer_id,   -- Unique identifier for each customer
    name,          -- Customer full name
    email,         -- Customer email address
    gender,        -- Customer gender
    signup_date,   -- Date when the customer signed up
    country        -- Customer country

-- Reference the customers table using dbt ref function
from {{ ref('customers') }}

-- Filter out rows where customer_id is null
where customer_id is not null
