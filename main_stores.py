# main_stores.py

from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

def main() -> None:
    """
    Orchestrates store data extraction from an API, cleaning, 
    and uploading to local Postgres database.
    """

    # 1) Prepare API endpoints and headers
    number_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    store_details_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
    headers = {
        "x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
    }

    # 2) Determine how many stores the API reports
    extractor = DataExtractor()
    num_stores = extractor.get_api_number_of_stores(number_stores_endpoint, headers)
    print(f"[INFO] Number of stores: {num_stores}")

    # 3) Retrieve store data from the API
    stores_df = extractor.get_store_details(store_details_endpoint, headers, num_stores)
    print(f"[INFO] Raw store data shape: {stores_df.shape}")

    # 4) Clean the store data
    cleaner = DataCleaning()
    cleaned_stores_df = cleaner.clean_store_data(stores_df)
    print(f"[INFO] Cleaned store data shape: {cleaned_stores_df.shape}")

    # 5) Upload to local DB
    local_connector = DatabaseConnector(creds_path="local_creds.yaml")
    local_connector.upload_to_db(cleaned_stores_df, "dim_store_details")
    print("[INFO] Store data uploaded to 'dim_store_details' in your local 'sales_data' DB!")

if __name__ == "__main__":
    main()
