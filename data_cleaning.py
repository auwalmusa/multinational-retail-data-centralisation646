# data_cleaning.py

import pandas as pd
import numpy as np

class DataCleaning:
    """
    The DataCleaning class contains methods for cleaning data extracted
    from various sources before loading into the database. This includes:
      - Handling missing values
      - Converting data types
      - Removing or fixing invalid characters
      - Other data transformations
    """

    def clean_user_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans user data by performing steps such as:
         - Removing or imputing null values
         - Parsing date columns
         - Converting columns to correct data types
         - Removing rows with invalid data
        """

        # Example: Drop any completely empty rows
        df.dropna(how="all", inplace=True)

        # Example: Replace empty strings or spaces in certain columns with NaN
        df.replace(r'^\s*$', np.nan, regex=True, inplace=True)

        # Example: Drop rows with too many null fields (threshold is adjustable)
        df.dropna(thresh=int(df.shape[1] * 0.5), inplace=True)

        # Example: If there's a "date_of_birth" column, parse to datetime
        if "date_of_birth" in df.columns:
            df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce")

        # Example: If there's an "email" column, remove rows with invalid email
        if "email" in df.columns:
            df = df[df["email"].str.contains("@", na=False)]

        # Example: Drop duplicates if needed
        df.drop_duplicates(inplace=True)

        # Return the cleaned DataFrame
        return df
