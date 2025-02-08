# main_orders.py

from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

def main() -> None:
    """
    Orchestrates extraction of orders data from AWS RDS,
    cleans it by removing columns like 'first_name', 'last_name', '1',
    and uploads to the local Postgres database.
    """
    # 1) Connect to AWS RDS
    rds_connector = DatabaseConnector(creds_path="db_creds.yaml")

    # 2) List tables to find the orders table
    tables = rds_connector.list_db_tables()
    print(f"[INFO] Tables in RDS: {tables}")

    # Example table name from your RDS
    orders_table_name = "orders_table"

    # 3) Extract orders from RDS
    extractor = DataExtractor()
    orders_df = extractor.read_rds_table(rds_connector, orders_table_name)
    print(f"[INFO] Raw orders data shape: {orders_df.shape}")

    # 4) Clean the orders data
    cleaner = DataCleaning()
    cleaned_orders_df = cleaner.clean_orders_data(orders_df)
    print(f"[INFO] Cleaned orders data shape: {cleaned_orders_df.shape}")

    # 5) Upload to local DB
    local_connector = DatabaseConnector(creds_path="local_creds.yaml")
    local_connector.upload_to_db(cleaned_orders_df, "orders_table")
    print("[INFO] Orders data uploaded to 'orders_table' in your local 'sales_data' DB!")

if __name__ == "__main__":
    main()
