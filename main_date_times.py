# main_date_times.py

from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

def main() -> None:
    """
    Orchestrates extraction of date/time data (JSON) from a public S3 link,
    cleaning it, and uploading to the local 'dim_date_times' table.
    """
    # 1) URL for the JSON file
    json_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"

    # 2) Extract the JSON data
    extractor = DataExtractor()
    date_times_df = extractor.retrieve_json_from_url(json_url)
    print(f"[INFO] Raw date_times data shape: {date_times_df.shape}")

    # 3) Clean the date/time data
    cleaner = DataCleaning()
    cleaned_dt_df = cleaner.clean_date_times_data(date_times_df)
    print(f"[INFO] Cleaned date_times data shape: {cleaned_dt_df.shape}")

    # 4) Upload to local DB
    local_connector = DatabaseConnector(creds_path="local_creds.yaml")
    local_connector.upload_to_db(cleaned_dt_df, "dim_date_times")
    print("[INFO] Date/time data uploaded to 'dim_date_times' in your local 'sales_data' DB!")

if __name__ == "__main__":
    main()
