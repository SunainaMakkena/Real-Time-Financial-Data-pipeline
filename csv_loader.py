import pandas as pd
import os
import logging
from typing import Optional, Dict, Any
from datetime import datetime
from utils.helpers import generate_filename, save_to_parquet

logger = logging.getLogger(__name__)


class CSVDataLoader:
    """
    Loader for CSV files with financial data.
    Handles common CSV formats for financial data.
    """

    def __init__(self):
        pass

    def load_stock_csv(self, file_path: str, symbol_col: str = None,
                       date_col: str = None) -> Optional[pd.DataFrame]:
        """
        Load stock price data from CSV file.

        Args:
            file_path: Path to CSV file
            symbol_col: Column name for stock symbol
            date_col: Column name for date/timestamp

        Returns:
            DataFrame with standardized stock data or None if error
        """
        try:
            logger.info(f"Loading stock data from CSV: {file_path}")

            # Try to auto-detect CSV format
            df = pd.read_csv(file_path)

            # Auto-detect date column if not specified
            if date_col is None:
                date_candidates = [col for col in df.columns if
                                   col.lower() in ['date', 'time', 'timestamp', 'datetime']]
                if date_candidates:
                    date_col = date_candidates[0]
                else:
                    logger.error(f"Cannot auto-detect date column in {file_path}")
                    return None

            # Auto-detect symbol column if not specified
            if symbol_col is None:
                symbol_candidates = [col for col in df.columns if
                                     col.lower() in ['symbol', 'ticker', 'stock', 'name']]
                if symbol_candidates:
                    symbol_col = symbol_candidates[0]

            # Try to convert date column to datetime
            try:
                df[date_col] = pd.to_datetime(df[date_col])
            except:
                logger.error(f"Failed to parse dates in {file_path}")
                return None

            # Rename date column to 'timestamp'
            df = df.rename(columns={date_col: 'timestamp'})

            # Auto-detect price columns
            price_mapping = {
                'open': [col for col in df.columns if col.lower() in ['open', 'opening', 'open_price']],
                'high': [col for col in df.columns if col.lower() in ['high', 'high_price']],
                'low': [col for col in df.columns if col.lower() in ['low', 'low_price']],
                'close': [col for col in df.columns if
                          col.lower() in ['close', 'closing', 'close_price', 'adj_close', 'adjusted_close']],
                'volume': [col for col in df.columns if col.lower() in ['volume', 'vol']]
            }

            # Rename matching columns
            column_mapping = {}
            for std_name, candidates in price_mapping.items():
                if candidates:
                    column_mapping[candidates[0]] = std_name

            df = df.rename(columns=column_mapping)

            # If we don't have a symbol column but filename contains a symbol, extract it
            if symbol_col is None:
                filename = os.path.basename(file_path)
                possible_symbol = filename.split('.')[0].split('_')[0]
                df['symbol'] = possible_symbol

            # Add metadata
            df['source'] = 'csv_import'

            # Ensure we have the required columns
            required_cols = ['timestamp', 'open', 'high', 'low', 'close']
            missing_cols = [col for col in required_cols if col not in df.columns]

            if missing_cols:
                logger.warning(f"Missing columns in {file_path}: {missing_cols}")
                # Fill in missing columns with NaN
                for col in missing_cols:
                    df[col] = float('nan')

            logger.info(f"Successfully loaded {len(df)} records from {file_path}")
            return df

        except pd.errors.ParserError as e:
            logger.error(f"CSV parsing error for {file_path}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error loading {file_path}: {str(e)}")
            return None

    def load_economic_csv(self, file_path: str, indicator_name: str,
                          date_col: str = None, value_col: str = None,
                          country: str = 'Unknown') -> Optional[pd.DataFrame]:
        """
        Load economic indicator data from CSV file.

        Args:
            file_path: Path to CSV file
            indicator_                        date_col: Column name for date/timestamp
                        value_col: Column name for the indicator value
                        country: Country for the indicator

        Returns:
            DataFrame with standardized economic data or None if error
        """
        try:
            logger.info(f"Loading economic data from CSV: {file_path}")

            # Load CSV
            df = pd.read_csv(file_path)

            # Auto-detect date column if not specified
            if date_col is None:
                date_candidates = [col for col in df.columns if
                                   col.lower() in ['date', 'time', 'timestamp', 'datetime']]
                if date_candidates:
                    date_col = date_candidates[0]
                else:
                    logger.error(f"Cannot auto-detect date column in {file_path}")
                    return None

            # Auto-detect value column if not specified
            if value_col is None:
                value_candidates = [col for col in df.columns if
                                    col.lower() in ['value', 'indicator', 'rate', 'price', 'amount']]
                if value_candidates:
                    value_col = value_candidates[0]
                else:
                    logger.error(f"Cannot auto-detect value column in {file_path}")
                    return None

            # Try to convert date column to datetime
            try:
                df[date_col] = pd.to_datetime(df[date_col])
            except:
                logger.error(f"Failed to parse dates in {file_path}")
                return None

            # Rename columns for consistency
            df = df.rename(columns={date_col: 'timestamp', value_col: 'value'})

            # Add metadata
            df['indicator'] = indicator_name
            df['country'] = country
            df['source'] = 'csv_import'

            logger.info(f"Successfully loaded {len(df)} records from {file_path}")
            return df

        except pd.errors.ParserError as e:
            logger.error(f"CSV parsing error for {file_path}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error loading {file_path}: {str(e)}")
            return None


def load_and_store_csv(file_path: str, data_type: str, bronze_layer: str, **kwargs: Any) -> dict:
    """
    Load CSV data and store in bronze layer.

    Args:
        file_path: Path to CSV file
        data_type: 'stock' or 'economic'
        bronze_layer: Path to bronze layer
        **kwargs: Additional parameters for specific loaders (e.g., symbol_col)

    Returns:
        Dictionary with operation results
    """
    loader = CSVDataLoader()

    if data_type == 'stock':
        df = loader.load_stock_csv(file_path, **kwargs)
    elif data_type == 'economic':
        df = loader.load_economic_csv(file_path, **kwargs)
    else:
        return {
            "status": "error",
            "message": f"Invalid data_type: {data_type}. Must be 'stock' or 'economic'.",
            "records_count": 0
        }

    if df is None or df.empty:
        return {
            "status": "error",
            "message": f"Failed to load data from {file_path}",
            "records_count": 0
        }

    # Generate filename and save to bronze layer
    filename = generate_filename("csv_import", data_type)
    file_path_parquet = save_to_parquet(df, bronze_layer, filename)

    return {
        "status": "success",
        "message": f"Successfully loaded and stored data from {file_path}",
        "records_count": len(df),
        "file_path": file_path_parquet
    }