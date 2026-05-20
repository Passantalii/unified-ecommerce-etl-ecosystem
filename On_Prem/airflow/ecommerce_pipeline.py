import pandas as pd
import pendulum

from airflow.sdk import dag, task
from pathlib import Path


# =========================================
# Base Folder
# =========================================

DATA_FOLDER = Path("/usr/local/airflow/include/raw_data")


# =========================================
# DAG Definition
# =========================================

@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 5, 17, tz="Africa/Cairo"),
    catchup=False,
    tags=["etl", "galaxy_schema", "ecommerce"],
)

def etl_galaxy_schema():

    # =====================================
    # Extract Tasks
    # =====================================

    @task()
    def extract_customers():

        df = pd.read_csv(DATA_FOLDER / "customers.csv")

        print("Customers Extracted")

        return df.to_dict(orient="records")

    @task()
    def extract_orders():

        df = pd.read_csv(DATA_FOLDER / "orders.csv")

        print("Orders Extracted")

        return df.to_dict(orient="records")

    @task()
    def extract_order_items():

        df = pd.read_csv(DATA_FOLDER / "order_items.csv")

        print("Order Items Extracted")

        return df.to_dict(orient="records")

    @task()
    def extract_products():

        df = pd.read_csv(DATA_FOLDER / "products.csv")

        print("Products Extracted")

        return df.to_dict(orient="records")

    @task()
    def extract_product_reviews():

        df = pd.read_csv(DATA_FOLDER / "product_reviews.csv")

        print("Product Reviews Extracted")

        return df.to_dict(orient="records")

    # =====================================
    # Transform Task
    # =====================================

    @task()
    def transform(
        customers,
        orders,
        order_items,
        products,
        product_reviews
    ):

        # =================================
        # Convert to DataFrames
        # =================================

        customers_df = pd.DataFrame(customers)

        orders_df = pd.DataFrame(orders)

        order_items_df = pd.DataFrame(order_items)

        products_df = pd.DataFrame(products)

        reviews_df = pd.DataFrame(product_reviews)

        # =================================
        # DIM_CUSTOMER
        # =================================

        dim_customer = customers_df.copy()

        dim_customer["customer_key"] = range(
            1,
            len(dim_customer) + 1
        )

        # Rename columns if needed
        rename_customer_cols = {}

        if "id" in dim_customer.columns:
            rename_customer_cols["id"] = "customer_id"

        if "signup_date" not in dim_customer.columns:
            dim_customer["signup_date"] = pd.Timestamp.today()

        dim_customer = dim_customer.rename(
            columns=rename_customer_cols
        )

        dim_customer["end_date"] = None

        dim_customer["is_current"] = True

        # =================================
        # DIM_PRODUCT
        # =================================

        dim_product = products_df.copy()

        dim_product["product_key"] = range(
            1,
            len(dim_product) + 1
        )

        rename_product_cols = {}

        if "id" in dim_product.columns:
            rename_product_cols["id"] = "product_id"

        dim_product = dim_product.rename(
            columns=rename_product_cols
        )

        if "category" not in dim_product.columns:
            dim_product["category"] = "Unknown"

        if "brand" not in dim_product.columns:
            dim_product["brand"] = "Unknown"

        if "price" not in dim_product.columns:
            dim_product["price"] = 0

        dim_product["effective_price_start_date"] = pd.Timestamp.today()

        dim_product["effective_price_end_date"] = None

        dim_product["is_current"] = True

        # =================================
        # DIM_DATE
        # =================================

        if "order_date" in orders_df.columns:

            date_series = pd.to_datetime(
                orders_df["order_date"]
            )

        else:

            date_series = pd.Series(
                pd.Timestamp.today()
            )

        dim_date = pd.DataFrame()

        dim_date["full_date"] = pd.to_datetime(
            date_series.unique()
        )

        dim_date["date_key"] = (
            dim_date["full_date"]
            .dt.strftime("%Y%m%d")
            .astype(int)
        )

        dim_date["year"] = dim_date["full_date"].dt.year

        dim_date["quarter"] = dim_date["full_date"].dt.quarter

        dim_date["month_number"] = dim_date["full_date"].dt.month

        dim_date["month_name"] = dim_date["full_date"].dt.month_name()

        dim_date["day_of_week"] = dim_date["full_date"].dt.dayofweek

        dim_date["day_name"] = dim_date["full_date"].dt.day_name()

        dim_date["is_weekend"] = (
            dim_date["day_of_week"] >= 5
        )

        dim_date["is_holiday"] = False

        # =================================
        # FACT_SALES
        # =================================

        fact_sales = order_items_df.merge(
            orders_df,
            on="order_id",
            how="left"
        )

        # =================================
        # Join Customer Key
        # =================================

        if (
            "customer_id" in fact_sales.columns
            and "customer_id" in dim_customer.columns
        ):

            fact_sales = fact_sales.merge(
                dim_customer[
                    ["customer_id", "customer_key"]
                ],
                on="customer_id",
                how="left"
            )

        # =================================
        # Join Product Key
        # =================================

        if (
            "product_id" in fact_sales.columns
            and "product_id" in dim_product.columns
        ):

            fact_sales = fact_sales.merge(
                dim_product[
                    ["product_id", "product_key"]
                ],
                on="product_id",
                how="left"
            )

        # =================================
        # Date Key
        # =================================

        if "order_date" in fact_sales.columns:

            fact_sales["order_date_key"] = (
                pd.to_datetime(
                    fact_sales["order_date"]
                )
                .dt.strftime("%Y%m%d")
                .astype(int)
            )

        else:

            fact_sales["order_date_key"] = (
                pd.Timestamp.today()
                .strftime("%Y%m%d")
            )

        # =================================
        # Measures
        # =================================

        if "quantity" not in fact_sales.columns:
            fact_sales["quantity"] = 1

        if "price" not in fact_sales.columns:
            fact_sales["price"] = 0

        fact_sales["total_amount"] = (
            fact_sales["quantity"]
            * fact_sales["price"]
        )

        if "payment_method" not in fact_sales.columns:
            fact_sales["payment_method"] = "Unknown"

        # =================================
        # FACT_SALES Final Columns
        # =================================

        sales_columns = [
            "order_item_id",
            "customer_key",
            "product_key",
            "order_date_key",
            "order_id",
            "quantity",
            "price",
            "total_amount",
            "payment_method"
        ]

        available_sales_columns = [
            col for col in sales_columns
            if col in fact_sales.columns
        ]

        fact_sales = fact_sales[
            available_sales_columns
        ]

        # =================================
        # FACT_REVIEWS
        # =================================

        fact_reviews = reviews_df.copy()

        # Customer Key
        if (
            "customer_id" in fact_reviews.columns
            and "customer_id" in dim_customer.columns
        ):

            fact_reviews = fact_reviews.merge(
                dim_customer[
                    ["customer_id", "customer_key"]
                ],
                on="customer_id",
                how="left"
            )

        # Product Key
        if (
            "product_id" in fact_reviews.columns
            and "product_id" in dim_product.columns
        ):

            fact_reviews = fact_reviews.merge(
                dim_product[
                    ["product_id", "product_key"]
                ],
                on="product_id",
                how="left"
            )

        # Review Date Key
        if "review_date" in fact_reviews.columns:

            fact_reviews["review_date_key"] = (
                pd.to_datetime(
                    fact_reviews["review_date"]
                )
                .dt.strftime("%Y%m%d")
                .astype(int)
            )

        else:

            fact_reviews["review_date_key"] = (
                pd.Timestamp.today()
                .strftime("%Y%m%d")
            )

        review_columns = [
            "review_id",
            "customer_key",
            "product_key",
            "review_date_key",
            "rating",
            "review_text"
        ]

        available_review_columns = [
            col for col in review_columns
            if col in fact_reviews.columns
        ]

        fact_reviews = fact_reviews[
            available_review_columns
        ]

        print("Transformation Completed Successfully!")

        # =================================
        # Return Output
        # =================================

        return {

            "dim_customer":
                dim_customer.to_dict(
                    orient="records"
                ),

            "dim_product":
                dim_product.to_dict(
                    orient="records"
                ),

            "dim_date":
                dim_date.to_dict(
                    orient="records"
                ),

            "fact_sales":
                fact_sales.to_dict(
                    orient="records"
                ),

            "fact_reviews":
                fact_reviews.to_dict(
                    orient="records"
                )
        }

    # =====================================
    # Load Task
    # =====================================

    @task()
    def load(transformed_data):

        pd.DataFrame(
            transformed_data["dim_customer"]
        ).to_csv(
            DATA_FOLDER / "dim_customer.csv",
            index=False
        )

        pd.DataFrame(
            transformed_data["dim_product"]
        ).to_csv(
            DATA_FOLDER / "dim_product.csv",
            index=False
        )

        pd.DataFrame(
            transformed_data["dim_date"]
        ).to_csv(
            DATA_FOLDER / "dim_date.csv",
            index=False
        )

        pd.DataFrame(
            transformed_data["fact_sales"]
        ).to_csv(
            DATA_FOLDER / "fact_sales.csv",
            index=False
        )

        pd.DataFrame(
            transformed_data["fact_reviews"]
        ).to_csv(
            DATA_FOLDER / "fact_reviews.csv",
            index=False
        )

        print("Galaxy Schema Created Successfully!")

    # =====================================
    # DAG Flow
    # =====================================

    customers_data = extract_customers()

    orders_data = extract_orders()

    order_items_data = extract_order_items()

    products_data = extract_products()

    product_reviews_data = extract_product_reviews()

    transformed = transform(
        customers_data,
        orders_data,
        order_items_data,
        products_data,
        product_reviews_data
    )

    load_task = load(transformed)

    [
        customers_data,
        orders_data,
        order_items_data,
        products_data,
        product_reviews_data
    ] >> transformed

    transformed >> load_task


# =========================================
# Instantiate DAG
# =========================================

etl_galaxy_schema()
