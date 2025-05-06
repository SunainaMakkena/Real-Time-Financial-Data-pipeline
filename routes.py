import io

from fastapi import APIRouter, HTTPException, BackgroundTasks, Response
from typing import List, Dict, Any, Optional
from datetime import datetime
from models.schema import (StockPrice, ForexRate, DataIngestionRequest,
                              DataIngestionResponse, TransformationRequest,
                              QueryRequest, DatasetInfo, CryptoPrice,
                              EconomicIndicator)
from sources.alphavantage import fetch_and_store_stock, AlphaVantageConnector
from sources.yahoo_finance import fetch_and_store_stock_yahoo, YahooFinanceConnector
from sources.csv_loader import load_and_store_csv
from services.transformers import transform_data_pipeline
from services.storage import DataLakeStorage
from sources.config import BRONZE_LAYER, SILVER_LAYER, GOLD_LAYER
from utils.helpers import generate_request_id
import logging
import os
import pandas as pd
import numpy as np

router = APIRouter()
storage = DataLakeStorage()
logger = logging.getLogger(__name__)


# --- Data Ingestion Endpoints ---
@router.post("/ingest", response_model=DataIngestionResponse)
async def ingest_data(request: DataIngestionRequest, background_tasks: BackgroundTasks):
    """
    Ingest data from various sources.
    """
    request_id = generate_request_id()
    logger.info(f"Received data ingestion request. Request ID: {request_id}")
    # Initialize df to None
    df = None
    if request.source == "alphavantage":
        if request.data_type == "stock":
            for symbol in request.symbols:
                background_tasks.add_task(fetch_and_store_stock, symbol, BRONZE_LAYER)
            return DataIngestionResponse(request_id=request_id, status="pending",
                                         message="Data ingestion for AlphaVantage stock data started.")

        elif request.data_type == "forex":
            # Example for fetching forex data (assuming pairs like "USD_EUR")
            connector = AlphaVantageConnector()
            for pair in request.symbols:
                try:
                    from_currency, to_currency = pair.split('_')
                except ValueError:
                    return DataIngestionResponse(request_id=request_id, status="error",
                                                 message=f"Invalid forex pair format: {pair}. Use format 'FROM_TO'",
                                                 errors=[f"Invalid forex pair: {pair}"])
                df = connector.fetch_forex_data(from_currency, to_currency)
                if df is not None and not df.empty:
                    filename = f"alphavantage_forex_{pair}_{datetime.now().strftime('%Y%m%d')}.parquet"
                    storage.save_dataset(df, "bronze", filename)

            return DataIngestionResponse(request_id=request_id, status="success",
                                         message="Forex data fetched and stored.",
                                         records_count=len(df) if df is not None else 0)

        elif request.data_type == "economic":
            connector = AlphaVantageConnector()
            for indicator in request.symbols:
                df = connector.fetch_economic_indicator(indicator)
                if df is not None and not df.empty:
                    filename = f"alphavantage_economic_{indicator}_{datetime.now().strftime('%Y%m%d')}.parquet"
                    storage.save_dataset(df, "bronze", filename)
            return DataIngestionResponse(request_id=request_id, status="success",
                                         message=f"Economic indicator data fetched and stored for {request.symbols}.",
                                         records_count=len(df) if df is not None else 0)

        else:
            return DataIngestionResponse(request_id=request_id, status="error",
                                         message=f"Unsupported data_type '{request.data_type}' for source '{request.source}'")


    elif request.source == "yahoo_finance":
        if request.data_type == "stock":
            for symbol in request.symbols:
                background_tasks.add_task(fetch_and_store_stock_yahoo, symbol, BRONZE_LAYER)
            return DataIngestionResponse(request_id=request_id, status="pending",
                                         message="Data ingestion for Yahoo Finance stock data started.")

        elif request.data_type == "crypto":
            connector = YahooFinanceConnector()
            for symbol in request.symbols:
                df = connector.fetch_crypto_data(symbol)
                if df is not None and not df.empty:
                    filename = f"yahoo_finance_crypto_{symbol}_{datetime.now().strftime('%Y%m%d')}.parquet"
                    storage.save_dataset(df, "bronze", filename)

            return DataIngestionResponse(request_id=request_id, status="success",
                                         message="Crypto data fetched and stored.",
                                         records_count=len(df) if df is not None else 0)
        else:
            return DataIngestionResponse(request_id=request_id, status="error",
                                         message=f"Unsupported data_type '{request.data_type}' for source '{request.source}'")


    elif request.source == "csv":
        if "file_path" not in request.params:
            return DataIngestionResponse(request_id=request_id, status="error",
                                         message="file_path parameter is required for CSV ingestion.")

        file_path = request.params["file_path"]

        if not os.path.exists(file_path):
            return DataIngestionResponse(request_id=request_id, status="error",
                                         message=f"File not found: {file_path}")

        if request.data_type == "stock":
            result = load_and_store_csv(file_path, "stock", BRONZE_LAYER, symbol_col=request.params.get("symbol_col"),
                                        date_col=request.params.get("date_col"))
        elif request.data_type == "economic":
            result = load_and_store_csv(file_path, "economic", BRONZE_LAYER,
                                        indicator_name=request.params.get("indicator_name"),
                                        date_col=request.params.get("date_col"),
                                        value_col=request.params.get("value_col"),
                                        country=request.params.get("country"))
        else:
            return DataIngestionResponse(request_id=request_id, status="error",
                                         message=f"Unsupported data_type: {request.data_type} for CSV source.")

        return DataIngestionResponse(request_id=request_id, status=result["status"], message=result["message"],
                                     records_count=result.get("records_count", 0))

    else:
        return DataIngestionResponse(request_id=request_id, status="error",
                                     message=f"Unsupported source: {request.source}")


# --- Data Transformation Endpoints ---
@router.post("/transform")
async def transform_data(request: TransformationRequest):
    """
    Transform data in the data lake.
    """
    logger.info(f"Received transformation request: {request}")

    result = transform_data_pipeline(f"data/{request.source_path}.parquet", request.destination_path,
                                     request.transformation_type, request.params)
    return result


# --- Data Querying and Retrieval Endpoints ---
@router.get("/datasets", response_model=List[str])
async def list_datasets(layer: str = "bronze"):
    """List available datasets in a specific layer."""
    # Return dataset names without .parquet extension
    return storage.list_datasets(layer)


@router.get("/datasets/{dataset_name}", response_model=DatasetInfo)
async def get_dataset_info(dataset_name: str, layer: str = "bronze"):
    """Get metadata for a specific dataset."""
    filename_for_storage = f"{dataset_name}.parquet"
    dataset_info = storage.get_dataset_info(filename_for_storage, layer)
    if dataset_info:
        if isinstance(dataset_info.get('first_date'), datetime):
             dataset_info['first_date'] = dataset_info['first_date'].isoformat()
        if isinstance(dataset_info.get('last_date'), datetime):
             dataset_info['last_date'] = dataset_info['last_date'].isoformat()
        if isinstance(dataset_info.get('created_at'), datetime):
             dataset_info['created_at'] = dataset_info['created_at'].isoformat()
        if isinstance(dataset_info.get('updated_at'), datetime):
             dataset_info['updated_at'] = dataset_info['updated_at'].isoformat()
        return dataset_info
    else:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' metadata not found in layer '{layer}'")


@router.get("/data/{layer}/{dataset_name}")
async def get_data(dataset_name: str, layer: str):
    """
    Retrieve data from a specific dataset.
    Returns data as JSON records.
    """
    filename_for_storage = f"{dataset_name}.parquet"
    logger.info(f"Attempting to retrieve data for: layer={layer}, dataset={filename_for_storage}")
    df = storage.read_dataset(filename_for_storage, layer)

    if df is None or df.empty:
        logger.error(f"Dataset not found or empty: layer={layer}, dataset={filename_for_storage}")
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found or empty in layer '{layer}'.")

    logger.info(f"Dataset read successfully: layer={layer}, dataset={filename_for_storage}, records={len(df)}")

    try:
        # --- FIX: Replace non-JSON compliant floats ---
        logger.debug(f"Replacing inf/-inf/NaN values in DataFrame for {dataset_name}")
        # Replace inf, -inf, and NaN with None (which becomes JSON null)
        df.replace([np.inf, -np.inf, np.nan], None, inplace=True)
        logger.debug(f"Finished replacing non-compliant float values for {dataset_name}")
        # --------------------------------------------

        # Convert datetime columns to ISO format string for JSON serialization
        logger.debug(f"Converting datetime columns for {dataset_name}")
        for col in df.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns:
            # Check if column exists before trying to access dt accessor
            if col in df.columns:
                # Convert to datetime objects first to handle potential strings/objects, then format
                # Use errors='coerce' to turn unparseable dates into NaT (which becomes None/null after replace)
                df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            else:
                 logger.warning(f"Column '{col}' selected as datetime but not found in DataFrame columns for {dataset_name}")
        logger.debug(f"Finished datetime conversion for {dataset_name}")


        # Convert DataFrame to list of dictionaries (records)
        logger.debug(f"Converting DataFrame to dict for {dataset_name}")
        records = df.to_dict(orient="records")
        logger.info(f"Successfully processed data for {dataset_name}, returning {len(records)} records.")
        return records # FastAPI automatically handles JSON response

    except Exception as e:
        # Log the specific error during processing
        logger.exception(f"!!! Error processing DataFrame or converting to dict for {dataset_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error processing data for {dataset_name}")


@router.get("/data/latest/{data_type}/{source}", response_model=List[Dict])
async def get_latest_data_endpoint(data_type: str, source: str, layer: str = "bronze"):
    """
    Retrieve the latest available data for a given data type and source.
    """
    df = storage.get_latest_data(data_type, source, layer) # Assumes get_latest_data handles filename logic correctly
    if df is None or df.empty:
        raise HTTPException(status_code=404, detail="No data found for the specified type and source.")
    for col in df.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns:
         df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    return df.to_dict(orient="records")



@router.get("/data/{layer}/{dataset_name}/download")
async def download_data(dataset_name: str, layer: str):
    """
    Retrieve data from a specific dataset as a CSV file download.
    """
    filename_for_storage = f"{dataset_name}.parquet"
    df = storage.read_dataset(filename_for_storage, layer)

    if df is None or df.empty:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found or empty in layer '{layer}'.")

    # Create CSV in memory
    output = io.StringIO()
    df.to_csv(output, index=False)
    output.seek(0)

    # Return as CSV response
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={dataset_name}.csv"}
    )

@router.delete("/datasets/{dataset_name}", status_code=204)
async def delete_dataset(dataset_name: str, layer: str = "bronze"):
    """Deletes a dataset."""
    success = storage.delete_dataset(dataset_name, layer)
    if not success:
        raise HTTPException(status_code=404, detail="Dataset not found or could not be deleted")
    return  # 204 No Content