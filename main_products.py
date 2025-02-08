# main_products.py

from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

def main() -> None:
    """
    Orchestrates product data extraction from an S3 CSV, 
    cleaning (including weight conversion), 
    and uploading to local Postgres database.
    """
    # 1) S3 path for products CSV
    s3_path = "s3://data-handling-public/products.csv"  # Example link

    # 2) Extract products data from S3
    extractor = DataExtractor()
    products_df = extractor.extract_from_s3(s3_path)
    print(f"[INFO] Raw products data shape: {products_df.shape}")

    # 3) Convert and clean product data
    cleaner = DataCleaning()
    products_df = cleaner.convert_product_weights(products_df)
    products_df = cleaner.clean_products_data(products_df)
    print(f"[INFO] Cleaned products data shape: {products_df.shape}")

    # 4) Upload to local DB
    local_connector = DatabaseConnector(creds_path="local_creds.yaml")
    local_connector.upload_to_db(products_df, "dim_products")
    print("[INFO] Products data uploaded to 'dim_products' in your local 'sales_data' DB!")

if __name__ == "__main__":
    main()
