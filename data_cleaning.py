# data_cleaning.py

import pandas as pd
import numpy as np

class DataCleaning:
    """
    The DataCleaning class contains methods for cleaning data extracted
    from various sources before loading into the database.
    """

    def clean_user_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the user data by removing or fixing invalid rows.
        This example may remove more rows than before:
          1. Replaces placeholders like 'NULL', '?', '--', 'missing' with NaN
          2. Drops rows if they have too many null values
          3. Converts 'date_of_birth' to datetime, drops rows with invalid dates
          4. Removes duplicates if any
        Adjust these steps based on your data requirements.
        """
        # 1. Replace placeholders with NaN
        df.replace(['NULL', '?', '--', 'missing'], pd.NA, inplace=True)

        # 2. Drop rows if they have too many null values (here, <80% valid data)
        threshold = int(df.shape[1] * 0.8)
        df.dropna(thresh=threshold, inplace=True)

        # 3. Convert 'date_of_birth' to datetime, drop rows with invalid or null dates
        if "date_of_birth" in df.columns:
            df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce")
            df.dropna(subset=["date_of_birth"], inplace=True)

        # 4. Drop duplicates if any
        df.drop_duplicates(inplace=True)

        print("Cleaned data shape:", df.shape)
        return df
