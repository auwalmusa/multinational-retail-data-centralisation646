# main_pdf.py

from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

def main():
    """
    Orchestrates extracting card data from a PDF link, cleaning it, 
    and then uploading it to your local 'sales_data' DB in 'dim_card_details'.
    """
    # 1) Initialize the extractor and cleaner
    extractor = DataExtractor()
    cleaner = DataCleaning()

    # 2) Extract PDF data
    pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    card_df = extractor.retrieve_pdf_data(pdf_link)
    print(f"[INFO] Raw card data shape: {card_df.shape}")
    print(card_df.head(5))  # Quick preview

    # 3) Clean the card data
    cleaned_card_df = cleaner.clean_card_data(card_df)
    print(f"[INFO] Cleaned card data shape: {cleaned_card_df.shape}")

    # 4) Upload to local DB (make sure 'local_creds.yaml' is correct)
    local_connector = DatabaseConnector(creds_path="local_creds.yaml")
    local_connector.upload_to_db(cleaned_card_df, "dim_card_details")
    print("[INFO] Card details uploaded to 'dim_card_details' in local 'sales_data' DB!")

if __name__ == "__main__":
    main()
