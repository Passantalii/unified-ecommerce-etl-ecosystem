{{ config(materialized='table') }}

-- DIM_CUSTOMER Dimension Table
-- This model creates the customer dimension table
-- with a surrogate key for the Data Warehouse.

select

    -- Surrogate Key generated inside the DWH
    row_number() over(order by customer_id) as customer_key,

    -- Business Key from source system
    customer_id,

    -- Customer information
    name,
    email,
    gender,
    country,
    signup_date,

    -- SCD Type 2 fields
    current_date as start_date,
    null as end_date,
    true as is_current

from {{ ref('customers') }}