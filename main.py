# main.py
# Full code example that:
# 1. Reads from AWS RDS (using db_creds.yaml) – read-only
# 2. Cleans data
# 3. Uploads to local PostgreSQL (using local_creds.yaml) – read/write

import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def main():
    # 1. Create a connector for the AWS RDS (READ-ONLY)
    #    Ensure you actually have "db_creds.yaml" in your folder with AWS credentials
    rds_connector = DatabaseConnector(creds_path="db_creds.yaml")

    # 2. Extract data from AWS RDS
    tables = rds_connector.list_db_tables()
    print("Tables in AWS RDS:", tables)

    user_table = "legacy_users"  # Change if the user data table has a different name
    extractor = DataExtractor()
    user_df = extractor.read_rds_table(rds_connector, user_table)
    print("Raw user data shape:", user_df.shape)

    # 3. Clean the extracted user data
    cleaner = DataCleaning()
    cleaned_user_df = cleaner.clean_user_data(user_df)
    print("Cleaned user data shape:", cleaned_user_df.shape)

    # 4. Create a connector for your local DB (READ-WRITE)
    #    Ensure "local_creds.yaml" has valid credentials for your local PostgreSQL
    local_connector = DatabaseConnector(creds_path="local_creds.yaml")

    # 5. Upload cleaned data to your local 'sales_data' DB in the table 'dim_users'
    local_connector.upload_to_db(cleaned_user_df, "dim_users")
    print("Data uploaded to 'dim_users' in your local 'sales_data' database!")

if __name__ == "__main__":
    main()
