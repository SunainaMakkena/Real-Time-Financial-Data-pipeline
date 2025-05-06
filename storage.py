import pandas as pd
import os
import logging
from typing import List, Dict
from utils.helpers import read_parquet, get_datasets_list, parse_dataset_info, generate_filename, save_to_parquet
from sources.config import BRONZE_LAYER, SILVER_LAYER, GOLD_LAYER
from datetime import datetime

logger = logging.getLogger(__name__)


class DataLakeStorage:
    """
    Manages data storage and retrieval in the data lake.
    """

    def __init__(self, bronze_layer: str = BRONZE_LAYER,
                 silver_layer: str = SILVER_LAYER,
                 gold_layer: str = GOLD_LAYER):
        self.bronze_layer = bronze_layer
        self.silver_layer = silver_layer
        self.gold_layer = gold_layer

        # Create layers if they don't exist
        os.makedirs(self.bronze_layer, exist_ok=True)
        os.makedirs(self.silver_layer, exist_ok=True)
        os.makedirs(self.gold_layer, exist_ok=True)

    def list_datasets(self, layer: str) -> List[str]:
        """
        List all datasets in a given layer.

        Args:
            layer: 'bronze', 'silver', or 'gold'

        Returns:
            List of dataset names (filenames without extension)
        """
        if layer == 'bronze':
            directory = self.bronze_layer
        elif layer == 'silver':
            directory = self.silver_layer
        elif layer == 'gold':
            directory = self.gold_layer
        else:
            logger.error(f"Invalid layer: {layer}")
            return []
        # get_datasets_list should return names without extension
        return get_datasets_list(directory)

    def get_dataset_info(self, filename_with_extension: str, layer: str) -> Dict:
        """
        Get metadata for a specific dataset. Expects full filename with extension.

        Args:
            filename_with_extension: Name of the dataset file (e.g., my_data.parquet)
            layer:  'bronze', 'silver', or 'gold'

        Returns:
            Dictionary containing dataset metadata or None if not found/error.
        """
        if layer == 'bronze':
            directory = self.bronze_layer
        elif layer == 'silver':
            directory = self.silver_layer
        elif layer == 'gold':
            directory = self.gold_layer
        else:
            logger.error(f"Invalid layer: {layer}")
            return None

        file_path = os.path.join(directory, filename_with_extension)

        if not os.path.exists(file_path):
            logger.error(f"Dataset file not found for info: {file_path}")
            return None

        try:
            info = parse_dataset_info(file_path, layer)
            return info
        except Exception as e:
            logger.error(f"Error parsing info for {file_path}: {e}")
            return None


    def read_dataset(self, filename_with_extension: str, layer: str) -> pd.DataFrame:
        """
        Read a dataset from the specified layer. Expects full filename with extension.

        Args:
            filename_with_extension: Name of the dataset file (e.g., my_data.parquet).
            layer: 'bronze', 'silver', or 'gold'.

        Returns:
            Pandas DataFrame containing the dataset, or empty DataFrame if not found/error.
        """
        if layer == 'bronze':
            directory = self.bronze_layer
        elif layer == 'silver':
            directory = self.silver_layer
        elif layer == 'gold':
            directory = self.gold_layer
        else:
            logger.error(f"Invalid layer: {layer}")
            return pd.DataFrame()

        file_path = os.path.join(directory, filename_with_extension)

        if not os.path.exists(file_path):
            logger.error(f"Dataset file not found: {file_path}")
            return pd.DataFrame()

        try:
            df = read_parquet(file_path)
            return df
        except Exception as e:
            logger.error(f"Error reading parquet file {file_path}: {e}")
            return pd.DataFrame()

    def save_dataset(self, df: pd.DataFrame, layer: str, filename: str) -> str:
        """
        Save a DataFrame to the specified layer. Assumes filename includes extension.

        Args:
            df: The DataFrame to save.
            layer: 'bronze', 'silver', or 'gold'.
            filename: Name to use when saving (e.g., my_data.parquet)

        Returns:
           Full file path or None on error
        """
        if layer == 'bronze':
            directory = self.bronze_layer
        elif layer == 'silver':
            directory = self.silver_layer
        elif layer == 'gold':
            directory = self.gold_layer
        else:
            logger.error(f"Invalid layer: {layer}")
            return None

        return save_to_parquet(df, directory, filename) # save_to_parquet handles path joining

    def get_latest_data(self, data_type: str, source: str, layer: str = "bronze") -> pd.DataFrame:
      """Retrieves the most recent data for a specific data type and source."""

      # List datasets *without* extension first
      datasets_base = self.list_datasets(layer)
      relevant_datasets_base = [
          dataset for dataset in datasets_base
          if source in dataset and data_type in dataset
      ]

      if not relevant_datasets_base:
          logger.warning(f"No data found for type '{data_type}' and source '{source}' in layer '{layer}'.")
          return pd.DataFrame()

      # Sort by date (assuming date is part of the filename, YYYYMMDD format)
      try:
          # Extract date part - robustness depends heavily on consistent naming!
          relevant_datasets_base.sort(key=lambda x: x.split('_')[-1], reverse=True)
      except IndexError:
          logger.error(f"Could not reliably sort datasets by date due to filename format in layer {layer}.")
          # Fallback: just take the first one found alphabetically (might not be latest)
          # Or return error / empty frame

      latest_dataset_base = relevant_datasets_base[0]
      latest_dataset_filename = f"{latest_dataset_base}.parquet" # Add extension back

      return self.read_dataset(latest_dataset_filename, layer)

    def delete_dataset(self, filename_with_extension: str, layer: str) -> bool:
        """Deletes a dataset file. Expects full filename with extension."""
        if layer == 'bronze':
            directory = self.bronze_layer
        elif layer == 'silver':
            directory = self.silver_layer
        elif layer == 'gold':
            directory = self.gold_layer
        else:
            logger.error(f"Invalid layer: {layer}")
            return False

        file_path = os.path.join(directory, filename_with_extension)

        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info(f"Deleted dataset: {file_path}")
                return True
            except OSError as e:
                logger.error(f"Error deleting dataset {file_path}: {e}")
                return False
        else:
            logger.warning(f"Attempted to delete non-existent dataset: {file_path}")
            return False # Or maybe True if deletion means "it's gone"? Depends on desired semantics.