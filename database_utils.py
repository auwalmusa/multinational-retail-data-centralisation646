# database_utils.py

import pandas as pd
import yaml
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine


class DatabaseConnector:
    """
    Handles database connection and data uploads for the project.
    """

    def __init__(self, creds_path: str = "local_creds.yaml") -> None:
        """
        Initializes the DatabaseConnector with a path to a YAML credentials file.

        Args:
            creds_path (str): Path to the YAML file containing DB credentials.
        """
        self._creds_path = creds_path

    def _read_db_creds(self) -> dict:
        """
        Reads the database credentials from the YAML file.

        Returns:
            dict: A dictionary containing the DB credentials.
        """
        with open(self._creds_path, "r", encoding="utf-8") as file:
            creds = yaml.safe_load(file)
        return creds

    def _create_db_engine(self) -> Engine:
        """
        Creates an SQLAlchemy engine using credentials from the YAML file.

        Returns:
            Engine: SQLAlchemy engine for the database.
        """
        creds = self._read_db_creds()
        db_url = (
            f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}"
            f"@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        )
        engine = create_engine(db_url)
        return engine

    def list_db_tables(self) -> list[str]:
        """
        Lists all table names in the connected database.

        Returns:
            list[str]: A list of table names.
        """
        engine = self._create_db_engine()
        inspector = inspect(engine)
        return inspector.get_table_names()

    def upload_to_db(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Uploads a pandas DataFrame to the specified table in the database.

        Args:
            df (pd.DataFrame): The DataFrame to upload.
            table_name (str): The name of the target table.
        """
        engine = self._create_db_engine()
        df.to_sql(name=table_name, con=engine, if_exists="append", index=False)
        print(f"[INFO] Data uploaded to table '{table_name}' successfully!")
