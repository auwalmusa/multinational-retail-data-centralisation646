# data_extraction.py

import pandas as pd
from database_utils import DatabaseConnector

class DataExtractor:
    """
    The DataExtractor class is responsible for extracting data from various sources.
    It includes:
     - CSV file extraction
     - API extraction
     - S3 extraction
     - RDS Database extraction (via DatabaseConnector)
    """

    def read_rds_table(self, db_connector: DatabaseConnector, table_name: str) -> pd.DataFrame:
        """
        Extracts an RDS database table into a Pandas DataFrame.
        :param db_connector: An instance of the DatabaseConnector class
        :param table_name: The name of the table to read
        :return: A Pandas DataFrame containing the table data
        """
        engine = db_connector.init_db_engine()
        df = pd.read_sql_table(table_name, con=engine)
        return df

    # You can add CSV, API, S3 extraction methods here in the future
