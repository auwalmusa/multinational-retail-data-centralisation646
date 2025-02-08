# database_utils.py

import yaml
import pandas as pd
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    def __init__(self, creds_path: str = "db_creds.yaml"):
        self.creds_path = creds_path

    def read_db_creds(self) -> dict:
        with open(self.creds_path, "r") as f:
            creds = yaml.safe_load(f)
        return creds

    def init_db_engine(self):
        creds = self.read_db_creds()
        db_url = (
            f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}"
            f"@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        )
        engine = create_engine(db_url)
        return engine

    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        return inspector.get_table_names()

    def upload_to_db(self, df: pd.DataFrame, table_name: str):
        engine = self.init_db_engine()
        df.to_sql(name=table_name, con=engine, if_exists="append", index=False)
        print(f"Data uploaded to table '{table_name}' successfully!")
