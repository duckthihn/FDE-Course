import pandas as pd
from dagster import asset, Output, Definitions, AssetIn, multi_asset, AssetOut
from resources.minio_io_manager import MinIOIOManager
from resources.mysql_io_manager import MySQLIOManager
from resources.psql_io_manager import PostgreSQLIOManager


@asset(
    io_manager_key="minio_io_manager", # output: save data to MinIO
    required_resource_keys={"mysql_io_manager"}, # input: load data from MySQL
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL"
)
def bronze_olist_orders_dataset(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM olist_orders_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    
    return Output(
        pd_data,
        metadata={
            "table": "olist_orders_dataset",
            "records count": len(pd_data),
        },
    )

@multi_asset(
    ins={
        "bronze_olist_orders_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        )
    },
    outs={
        "olist_orders_dataset": AssetOut(
            io_manager_key="psql_io_manager", # output: save data to PostgreSQL
            key_prefix=["warehouse", "public"],
            metadata={
                "primary key": [
                    "order_id",
                    "customer_id",
                ],
                "columns": [
                    "order_id",
                    "customer_id",
                    "order_status",
                    "order_purchase_timestamp",
                    "order_approved_at",
                    "order_delivered_carrier_date",
                    "order_delivered_customer_date",
                    "order_estimated_delivery_date",
                ]
            },
        )
    },
    compute_kind="PostgreSQL"
)
def olist_orders_dataset(bronze_olist_orders_dataset) -> Output[pd.DataFrame]:
    return Output(
        bronze_olist_orders_dataset,
        metadata={
            "schema": "public",
            "table": "bronze_olist_orders_dataset",
            "records counts": len(bronze_olist_orders_dataset),
        },
    )


MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "database": "brazillian_ecommerce",
    "user": "admin",
    "password": "admin123",
}

MINIO_CONFIG = {
    "endpoint_url": "localhost:9000",
    "bucket_name": "warehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
}

PSQL_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "postgres",
    "user": "admin",
    "password": "admin123",
}


defs = Definitions(
    assets=[bronze_olist_orders_dataset, olist_orders_dataset],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
    },
)
