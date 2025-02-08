# data_cleaning.py

import pandas as pd
import numpy as np
import re
from typing import Optional


class DataCleaning:
    """
    A collection of methods to clean data from users, cards, stores, orders, and products.
    """

    def clean_user_data(self, user_df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans user data by removing invalid placeholders, dropping duplicates, etc.

        Args:
            user_df (pd.DataFrame): Raw user data.

        Returns:
            pd.DataFrame: Cleaned user data.
        """
        user_df.replace(['NULL', '?', '--', 'missing'], np.nan, inplace=True)
        threshold = int(user_df.shape[1] * 0.8)
        user_df.dropna(thresh=threshold, inplace=True)
        if "date_of_birth" in user_df.columns:
            user_df["date_of_birth"] = pd.to_datetime(user_df["date_of_birth"], errors="coerce")
            user_df.dropna(subset=["date_of_birth"], inplace=True)
        user_df.drop_duplicates(inplace=True)
        return user_df

    def clean_card_data(self, card_df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans card data from PDF. Removes duplicates, placeholders, etc.

        Args:
            card_df (pd.DataFrame): Raw card data.

        Returns:
            pd.DataFrame: Cleaned card data.
        """
        card_df.replace(['NULL', '?', '--', 'missing'], np.nan, inplace=True)
        threshold = int(card_df.shape[1] * 0.8)
        card_df.dropna(thresh=threshold, inplace=True)
        card_df.drop_duplicates(inplace=True)
        return card_df

    def clean_store_data(self, store_df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans store data from the API, removing duplicates and placeholders.

        Args:
            store_df (pd.DataFrame): Raw store data.

        Returns:
            pd.DataFrame: Cleaned store data.
        """
        store_df.replace(['NULL', '?', '--', 'missing'], np.nan, inplace=True)
        threshold = int(store_df.shape[1] * 0.8)
        store_df.dropna(thresh=threshold, inplace=True)
        store_df.drop_duplicates(inplace=True)
        return store_df

    def convert_product_weights(self, product_df: pd.DataFrame) -> pd.DataFrame:
        """
        Converts weight column from various units (g, kg, ml) to kilograms.

        Args:
            product_df (pd.DataFrame): Product data with 'weight' column.

        Returns:
            pd.DataFrame: Same DataFrame, but with a new 'weight_kg' column.
        """

        def _weight_to_kg(weight_str: Optional[str]) -> float:
            """Helper to parse weight strings into kg as floats."""
            if not isinstance(weight_str, str):
                return np.nan

            text = weight_str.strip().lower()
            match = re.match(r"([0-9]*\.?[0-9]+)", text)
            if not match:
                return np.nan

            value = float(match.group(1))
            if "kg" in text:
                return value
            if "ml" in text or "g" in text:
                return value / 1000.0
            return np.nan

        if "weight" in product_df.columns:
            product_df["weight_kg"] = product_df["weight"].apply(_weight_to_kg)
        else:
            print("[WARN] 'weight' column not found in product DataFrame.")

        return product_df

    def clean_products_data(self, product_df: pd.DataFrame) -> pd.DataFrame:
        """
        General cleaning for product data after weight conversion.

        Args:
            product_df (pd.DataFrame): Product data.

        Returns:
            pd.DataFrame: Cleaned product data.
        """
        product_df.replace(['NULL', '?', '--', 'missing'], np.nan, inplace=True)
        threshold = int(product_df.shape[1] * 0.8)
        product_df.dropna(thresh=threshold, inplace=True)
        product_df.drop_duplicates(inplace=True)
        return product_df

    def clean_orders_data(self, orders_df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans orders data. Removes unwanted columns, placeholders, duplicates.

        Args:
            orders_df (pd.DataFrame): Raw orders table.

        Returns:
            pd.DataFrame: Cleaned orders DataFrame.
        """
        cols_to_drop = ["first_name", "last_name", "1"]
        for col in cols_to_drop:
            if col in orders_df.columns:
                orders_df.drop(columns=col, inplace=True)

        orders_df.replace(['NULL', '?', '--', 'missing'], np.nan, inplace=True)
        threshold = int(orders_df.shape[1] * 0.8)
        orders_df.dropna(thresh=threshold, inplace=True)
        orders_df.drop_duplicates(inplace=True)
        return orders_df

    def clean_date_times_data(self, dt_df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans date/time data from a JSON file. Converts columns, removes duplicates.

        Args:
            dt_df (pd.DataFrame): Raw date/time data.

        Returns:
            pd.DataFrame: Cleaned date/time data.
        """
        dt_df.replace(['NULL', '?', '--', 'missing'], np.nan, inplace=True)

        # Example: parse 'timestamp' or 'date' columns
        if "timestamp" in dt_df.columns:
            dt_df["timestamp"] = pd.to_datetime(dt_df["timestamp"], errors="coerce")
        if "date" in dt_df.columns:
            dt_df["date"] = pd.to_datetime(dt_df["date"], errors="coerce")

        threshold = int(dt_df.shape[1] * 0.8)
        dt_df.dropna(thresh=threshold, inplace=True)
        dt_df.drop_duplicates(inplace=True)
        return dt_df
