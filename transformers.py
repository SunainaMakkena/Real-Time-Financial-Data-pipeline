from datetime import datetime
import pandas as pd
import logging
from typing import Dict, Any, Optional
from utils.helpers import read_parquet, save_to_parquet, generate_filename

logger = logging.getLogger(__name__)

class DataTransformer:
    """
    Class for data transformation operations.
    """

    def __init__(self):
        pass

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform data cleaning operations.

        Args:
            df: Input DataFrame

        Returns:
            Cleaned DataFrame
        """
        logger.info("Performing data cleaning...")

        # 1. Drop rows with any missing values
        cleaned_df = df.dropna()

        # 2. Remove duplicate rows
        cleaned_df = cleaned_df.drop_duplicates()

        # 3. Convert timestamp to datetime objects if not already
        if 'timestamp' in cleaned_df.columns:
            try:
                cleaned_df['timestamp'] = pd.to_datetime(cleaned_df['timestamp'])
            except:
                logger.warning("Timestamp column could not be converted to datetime.")
        
        # 4.  Basic type conversions
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'value']
        for col in numeric_cols:
            if col in cleaned_df.columns:
                try:
                    cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors='coerce')
                except:
                    logger.warning(f"Column {col} could not be converted to numeric.")
        
        
        logger.info(f"Data cleaning complete. Rows before: {len(df)}, Rows after: {len(cleaned_df)}")
        return cleaned_df


    def normalize_data(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """
        Normalize data to a standard format.

        Args:
            df: Input DataFrame
            data_type: Type of data ('stock', 'forex', 'economic', 'crypto')

        Returns:
            Normalized DataFrame
        """
        logger.info(f"Performing data normalization for data_type: {data_type}")

        normalized_df = df.copy()
        
        if data_type == 'stock':
            # Stock data normalization (example: ensure required columns)
            required_cols = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'source']
            missing_cols = [col for col in required_cols if col not in normalized_df.columns]
            if missing_cols:
              logger.warning(f"Missing columns during normalization: {missing_cols}")
        
        elif data_type == 'forex':
            # Forex data normalization (example: calculate mid-rate)
            required_cols = ['timestamp', 'from_currency', 'to_currency', 'rate', 'source']
            if 'open' in normalized_df and 'close' in normalized_df:
                normalized_df['mid_rate'] = (normalized_df['open'] + normalized_df['close']) / 2
            
        elif data_type == 'economic':
            # Economic data normalization
            required_cols = ['timestamp', 'indicator', 'value', 'country', 'source']
        
        elif data_type == 'crypto':
             # Crypto data normalization
            required_cols = ['timestamp', 'symbol', 'price', 'source']


        # Add a data_type column for easier filtering later
        normalized_df['data_type'] = data_type

        logger.info(f"Data normalization complete for {data_type}.")
        return normalized_df

    def aggregate_data(self, df: pd.DataFrame,
                       time_period: str = 'M',
                       agg_columns: Optional[Dict[str, str]] = None
                      ) -> pd.DataFrame:
        """
        Aggregate data by a given time period.

        Args:
            df: Input DataFrame
            time_period:  'D' (daily), 'W' (weekly), 'M' (monthly), 'Y' (yearly)
            agg_columns: Dictionary mapping columns to aggregation functions (e.g., {'volume': 'sum', 'close': 'last'})

        Returns:
            Aggregated DataFrame
        """
        logger.info(f"Performing data aggregation. Time period: {time_period}")

        if 'timestamp' not in df.columns:
            logger.error("Timestamp column not found for aggregation.")
            return pd.DataFrame()  # Return empty DataFrame on error
        
        if agg_columns is None:
            # Default aggregation
            agg_columns = {
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }

        # Filter agg_columns to only include those present in the DataFrame
        valid_agg_columns = {col: func for col, func in agg_columns.items() if col in df.columns}


        # Ensure timestamp is datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Group and aggregate
        aggregated_df = df.groupby([pd.Grouper(key='timestamp', freq=time_period)]).agg(valid_agg_columns)
        aggregated_df = aggregated_df.reset_index()
        
        logger.info("Data aggregation complete.")
        return aggregated_df


def transform_data_pipeline(source_path: str, destination_path: str, transformation_type: str,
                            params: Dict[str, Any] = None) -> dict:
    """
    Orchestrates the data transformation process.

    Args:
        source_path: Path to source data
        destination_path: Path to store transformed data
        transformation_type: 'clean', 'normalize', 'aggregate'
        params: Additional parameters for transformations (e.g., data_type, time_period)

    Returns:
        Dictionary with operation results
    """
    transformer = DataTransformer()

    # Read data from source
    df = read_parquet(source_path)
    if df.empty:
        return {
            "status": "error",
            "message": f"Failed to read data from {source_path}",
            "records_count": 0
        }

    # Apply transformation
    if transformation_type == 'clean':
        transformed_df = transformer.clean_data(df)
    elif transformation_type == 'normalize':
        if 'data_type' not in params:
            return {"status": "error", "message": "data_type parameter is required for normalization."}
        transformed_df = transformer.normalize_data(df, params['data_type'])
    elif transformation_type == 'aggregate':
        time_period = params.get('time_period', 'M')
        agg_columns = params.get('agg_columns', None)  # Pass agg_columns parameter
        transformed_df = transformer.aggregate_data(df, time_period, agg_columns)
    else:
        return {
            "status": "error",
            "message": f"Invalid transformation_type: {transformation_type}",
            "records_count": 0
        }
    
    if transformed_df.empty:
         return {
            "status": "error",
            "message": f"Transformation resulted in empty dataframe.",
            "records_count": 0
        }

    # Generate destination filename
    filename_parts = source_path.split('/')[-1].split('.')[0].split('_')
    source = filename_parts[0] if len(filename_parts) > 0 else "unknown"
    data_type_file = filename_parts[1] if len(filename_parts) > 1 else "data"

    
    if transformation_type == 'aggregate':
      destination_filename = generate_filename(f"{source}_{transformation_type}", data_type_file, datetime.now().strftime("%Y%m%d"))
    else:
      destination_filename = generate_filename(source, f"{data_type_file}_{transformation_type}")

    # Save transformed data
    file_path = save_to_parquet(transformed_df, destination_path, destination_filename)

    return {
        "status": "success",
        "message": f"Successfully transformed data. Transformation type: {transformation_type}",
        "records_count": len(transformed_df),
        "file_path": file_path
    }