# data_extraction.py

import os
import re
import time
import json
import requests
import pandas as pd
import tabula
import boto3
from requests import Session
from database_utils import DatabaseConnector
from sqlalchemy.engine import Engine


class DataExtractor:
    """
    Provides methods to extract data from various sources, including:
      - RDS databases
      - PDF files
      - AWS S3 CSV
      - JSON from URLs
      - External APIs
    """

    def read_rds_table(self, db_connector: DatabaseConnector, table_name: str) -> pd.DataFrame:
        """
        Reads a table from an RDS database into a pandas DataFrame.

        Args:
            db_connector (DatabaseConnector): An instance of the DatabaseConnector class.
            table_name (str): The name of the table to read.

        Returns:
            pd.DataFrame: The table data as a pandas DataFrame.
        """
        engine = db_connector._create_db_engine()  # Access the private method to get Engine
        df = pd.read_sql_table(table_name, con=engine)
        return df

    def retrieve_pdf_data(self, pdf_url: str) -> pd.DataFrame:
        """
        Extracts data from a PDF (all pages) using tabula-py.

        Args:
            pdf_url (str): The URL or file path to the PDF.

        Returns:
            pd.DataFrame: Concatenated DataFrame of all pages.
        """
        pdf_dfs = tabula.read_pdf(pdf_url, pages="all")
        combined_df = pd.concat(pdf_dfs, ignore_index=True)
        return combined_df

    def extract_from_s3(self, s3_path: str) -> pd.DataFrame:
        """
        Downloads a CSV file from an S3 path and loads it into a pandas DataFrame.

        Args:
            s3_path (str): The S3 URI, e.g. 's3://bucket-name/folder/products.csv'.

        Returns:
            pd.DataFrame: DataFrame loaded from the CSV in S3.
        """
        # Parse the S3 path
        path_parts = s3_path.replace("s3://", "").split("/", 1)
        bucket_name = path_parts[0]
        object_key = path_parts[1]

        s3_client = boto3.client("s3")
        temp_filename = "temp_s3_file.csv"
        s3_client.download_file(bucket_name, object_key, temp_filename)

        df = pd.read_csv(temp_filename)
        os.remove(temp_filename)

        return df

    def retrieve_json_from_url(self, json_url: str) -> pd.DataFrame:
        """
        Downloads JSON data from a URL and returns it as a DataFrame.

        Args:
            json_url (str): The URL to the JSON file.

        Returns:
            pd.DataFrame: A DataFrame parsed from the JSON structure.
        """
        response = requests.get(json_url, timeout=30)
        response.raise_for_status()
        data = json.loads(response.text)
        return pd.DataFrame(data)

    def get_api_number_of_stores(self, api_endpoint: str, headers: dict) -> int:
        """
        Retrieves the number of stores from an API endpoint.

        Args:
            api_endpoint (str): Endpoint returning JSON with 'number_stores' key.
            headers (dict): Headers for the request (e.g. x-api-key).

        Returns:
            int: The total number of stores reported by the API.
        """
        response = requests.get(api_endpoint, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("number_stores", 0)

    def get_store_details(self, store_endpoint: str, headers: dict, number_of_stores: int) -> pd.DataFrame:
        """
        Loops through each store number to fetch store details from the API.

        Args:
            store_endpoint (str): Endpoint format, e.g. 'https://api/store_details/{store_number}'.
            headers (dict): Request headers with x-api-key.
            number_of_stores (int): Number of stores to iterate through.

        Returns:
            pd.DataFrame: DataFrame containing combined store info.
        """
        session = Session()
        all_stores = []

        for store_num in range(1, number_of_stores + 1):
            # Hardcode skipping known broken store if necessary
            if store_num == 451:
                print("[INFO] Skipping store #451 due to known 500 error.")
                continue

            url = store_endpoint.format(store_number=store_num)
            try:
                resp = session.get(url, headers=headers, timeout=30)
                resp.raise_for_status()
                store_data = resp.json()
                all_stores.append(store_data)
            except requests.exceptions.HTTPError as exc:
                print(f"[WARN] Skipping store {store_num} - server error: {exc}")
                continue
            except requests.exceptions.RequestException as exc:
                print(f"[ERROR] Store {store_num} request failed: {exc}")
                continue

            time.sleep(0.1)

        return pd.DataFrame(all_stores)
