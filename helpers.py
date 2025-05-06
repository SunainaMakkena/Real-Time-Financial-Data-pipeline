import os
import uuid
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import logging
from pathlib import Path
from typing import Dict, Optional  # Import Dict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_request_id():
    """Generate a unique request ID."""
    return str(uuid.uuid4())


def generate_filename(source: str, data_type: str, date_str: str = None):
    """Generate a standardized filename for data storage."""
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")
    # Ensure .parquet extension is added here
    return f"{source}_{data_type}_{date_str}.parquet"


def save_to_parquet(df: pd.DataFrame, path: str, filename: str):
    """Save dataframe to parquet format. Assumes filename includes extension."""
    full_path = os.path.join(path, filename)

    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(full_path), exist_ok=True)

    try:
        # Convert to pyarrow table and write to parquet
        table = pa.Table.from_pandas(df, preserve_index=False) # Often don't need pandas index in parquet
        pq.write_table(table, full_path)
        logger.info(f"Saved {len(df)} records to {full_path}")
        return full_path
    except Exception as e:
        logger.error(f"Error saving parquet file {full_path}: {e}")
        return None


def read_parquet(file_path: str) -> pd.DataFrame:
    """Read a parquet file into a pandas dataframe."""
    if not os.path.exists(file_path):
        # Logged in storage layer, no need to repeat error here usually
        # logger.error(f"File not found: {file_path}")
        return pd.DataFrame()

    try:
        table = pq.read_table(file_path)
        df = table.to_pandas()
        logger.info(f"Read {len(df)} records from {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error reading parquet file {file_path}: {e}")
        return pd.DataFrame()


def get_datasets_list(directory: str) -> list:
    """Get a list of all .parquet dataset names (without extension) in a directory."""
    path = Path(directory)
    if not path.exists():
        logger.warning(f"Dataset directory not found: {directory}")
        return []

    datasets = []
    # Use glob to find only .parquet files directly within the directory (not recursive needed here)
    for file_path in path.glob("*.parquet"):
        # Get filename without extension
        dataset_name = file_path.stem
        datasets.append(dataset_name)

    logger.info(f"Found {len(datasets)} datasets in {directory}")
    return datasets


def parse_dataset_info(file_path: str, layer: str) -> Optional[Dict]: # Use Optional[Dict]
    """Parse dataset info from a parquet file path."""
    if not os.path.exists(file_path):
        logger.error(f"Cannot parse info, file not found: {file_path}")
        return None

    try:
        # Extract base name for the 'name' field
        base_filename = os.path.basename(file_path)
        dataset_name_no_ext = os.path.splitext(base_filename)[0]

        # Attempt to parse source, type, date from name (requires consistent naming)
        name_parts = dataset_name_no_ext.split('_')
        source = name_parts[0] if len(name_parts) > 0 else "unknown"
        data_type = name_parts[1] if len(name_parts) > 1 else "unknown"
        # date_str = name_parts[-1] # Assuming date is always last part

        # Read metadata WITHOUT loading full data if possible (more efficient)
        parquet_file = pq.ParquetFile(file_path)
        metadata = parquet_file.metadata
        schema = parquet_file.schema.to_arrow_schema()

        record_count = metadata.num_rows

        # Get schema details
        columns = [field.name for field in schema]

        # Try to find date columns based on common names or types
        date_col = None
        for field in schema:
            if ('date' in field.name.lower() or 'time' in field.name.lower()) and pa.types.is_timestamp(field.type):
                date_col = field.name
                break
        # If no timestamp found, check for string dates (less ideal)
        if not date_col:
             for field in schema:
                if ('date' in field.name.lower() or 'time' in field.name.lower()) and pa.types.is_string(field.type):
                    date_col = field.name
                    break

        first_date = None
        last_date = None
        if date_col:
            # Reading just one column is faster than loading the whole thing
            # Note: This still loads the column data, could be slow for huge files.
            # For very large files, consider storing min/max in Parquet metadata itself.
            try:
                date_series = pq.read_table(file_path, columns=[date_col])[date_col].to_pandas()
                # Convert to datetime if it's not already (e.g., if read as object/string)
                date_series = pd.to_datetime(date_series, errors='coerce')
                first_date = date_series.min()
                last_date = date_series.max()
                # Convert NaT to None for JSON compatibility
                first_date = first_date if pd.notna(first_date) else None
                last_date = last_date if pd.notna(last_date) else None
            except Exception as e:
                logger.warning(f"Could not read or parse date column '{date_col}' for stats in {file_path}: {e}")


        # Get symbols if available
        symbol_col = next((col for col in columns if col.lower() in ['symbol', 'ticker', 'name']), None)
        symbols = []
        if symbol_col:
            try:
                # Reading unique values of one column
                symbol_series = pq.read_table(file_path, columns=[symbol_col])[symbol_col].to_pandas()
                symbols = symbol_series.unique().tolist()
                 # Limit number of symbols shown for performance/display reasons if necessary
                if len(symbols) > 50:
                    symbols = symbols[:50] + ['...']
            except Exception as e:
                 logger.warning(f"Could not read symbol column '{symbol_col}' for stats in {file_path}: {e}")


        file_stats = os.stat(file_path)
        created_at = datetime.fromtimestamp(file_stats.st_ctime)
        updated_at = datetime.fromtimestamp(file_stats.st_mtime)

        return {
            "name": dataset_name_no_ext,
            "data_type": data_type,
            "source": source,
            "record_count": record_count,
            "first_date": first_date, # Already datetime objects or None
            "last_date": last_date,   # Already datetime objects or None
            "symbols": symbols,
            "layer": layer,
            "file_path": file_path, # Maybe remove this? Internal detail.
            "created_at": created_at, # Already datetime objects
            "updated_at": updated_at  # Already datetime objects
        }
    except Exception as e:
        logger.error(f"Error parsing dataset info for {file_path}: {str(e)}")
        return None