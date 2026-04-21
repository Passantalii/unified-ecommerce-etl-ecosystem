# Data Warehouse Architecture

This diagram illustrates the **Galaxy Schema** designed for the Unified E-commerce ETL Ecosystem. It supports multi-source data integration and maintains historical data using SCD Type 2.

```mermaid
erDiagram
    DIM_CUSTOMER ||--o{ FACT_SALES : "purchases"
    DIM_CUSTOMER ||--o{ FACT_REVIEWS : "writes"
    DIM_PRODUCT ||--o{ FACT_SALES : "is sold"
    DIM_PRODUCT ||--o{ FACT_REVIEWS : "is reviewed"
    DIM_DATE ||--o{ FACT_SALES : "order_date"
    DIM_DATE ||--o{ FACT_REVIEWS : "review_date"

    DIM_CUSTOMER {
        int customer_key PK
        int customer_id
        string name
        string email
        string gender
        string country
        date start_date
        date end_date
        boolean is_current
    }

    DIM_PRODUCT {
        int product_key PK
        int product_id
        string product_name
        string category
        string brand
        decimal price
        date effective_price_start_date
        date effective_price_end_date
        boolean is_current
    }

    DIM_DATE {
        int date_key PK
        date full_date
        int year
        int quarter
        int month_number
        string month_name
        int day_of_week
        string day_name
        boolean is_weekend
    }

    FACT_SALES {
        int order_item_id PK
        int customer_key FK
        int product_key FK
        int order_date_key FK
        int order_id
        int quantity
        decimal unit_price
        decimal total_amount
        string payment_method
    }

    FACT_REVIEWS {
        int review_id PK
        int customer_key FK
        int product_key FK
        int review_date_key FK
        int rating
        string review_text
    }
```
