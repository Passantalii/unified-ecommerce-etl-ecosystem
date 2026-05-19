{{ config(materialized='table') }}

-- Date Dimension Table

select distinct

    cast(strftime(order_date, '%Y%m%d') as integer) as date_key,

    order_date as full_date,

    year(order_date) as year,

    quarter(order_date) as quarter,

    month(order_date) as month_number,

    monthname(order_date) as month_name,

    dayofweek(order_date) as day_of_week,

    dayname(order_date) as day_name,

    case
        when dayofweek(order_date) in (0,6) then true
        else false
    end as is_weekend,

    false as is_holiday

from {{ ref('orders') }}