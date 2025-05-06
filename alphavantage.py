import requests
import pandas as pd
from datetime import datetime
import logging
from typing import Dict, Any, List, Optional
from .config import ALPHAVANTAGE_API_KEY
from utils.helpers import generate_filename, save_to_parquet

logger = logging.getLogger(__name__)

BASE_URL = "https://www.alphavantage.co/query"


class AlphaVantageConnector:
    """
    Connector for Alpha Vantage financial data API.
    """
    def __init__(self, api_key: str = ALPHAVANTAGE_API_KEY):
        self.api_key = api_key
    
    def fetch_stock_data(self, symbol: str, output_size: str = "full") -> Optional[pd.DataFrame]:
        """
        Fetch daily stock data for a specific symbol.
        
        Args:
            symbol: Stock ticker symbol
            output_size: 'compact' (latest 100 points) or 'full' (20+ years of data)
            
        Returns:
            DataFrame with stock price data or None if error
        """
        try:
            params = {
                "function": "TIME_SERIES_DAILY",
                "symbol": symbol,
                "outputsize": output_size,
                "datatype": "json",
                "apikey": self.api_key
            }
            
            logger.info(f"Fetching stock data for {symbol}")
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API error messages
            if "Error Message" in data:
                logger.error(f"API Error: {data['Error Message']}")
                return None
                
            if "Time Series (Daily)" not in data:
                logger.error(f"Unexpected response format: {data.keys()}")
                return None
                
            # Convert to DataFrame
            time_series = data["Time Series (Daily)"]
            df = pd.DataFrame.from_dict(time_series, orient="index")
            
            # Rename columns
            df.rename(columns={
                "1. open": "open",
                "2. high": "high",
                "3. low": "low",
                "4. close": "close",
                "5. volume": "volume"
            }, inplace=True)
            
            # Convert types
            for col in ["open", "high", "low", "close"]:
                df[col] = pd.to_numeric(df[col])
            df["volume"] = pd.to_numeric(df["volume"], downcast="integer")
            
            # Add symbol and reset index
            df.index = pd.to_datetime(df.index)
            df = df.reset_index()
            df.rename(columns={"index": "timestamp"}, inplace=True)
            df["symbol"] = symbol
            df["source"] = "alphavantage"
            
            logger.info(f"Successfully fetched {len(df)} records for {symbol}")
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for {symbol}: {str(e)}")
            return None
        except ValueError as e:
            logger.error(f"JSON parsing error for {symbol}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching {symbol}: {str(e)}")
            return None
    
    def fetch_forex_data(self, from_currency: str, to_currency: str) -> Optional[pd.DataFrame]:
        """
        Fetch daily foreign exchange rates.
        
        Args:
            from_currency: From currency code (e.g., USD)
            to_currency: To currency code (e.g., EUR)
            
        Returns:
            DataFrame with forex data or None if error
        """
        try:
            params = {
                "function": "FX_DAILY",
                "from_symbol": from_currency,
                "to_symbol": to_currency,
                "outputsize": "full",
                "datatype": "json",
                "apikey": self.api_key
            }
            
            logger.info(f"Fetching forex data for {from_currency}/{to_currency}")
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API error messages
            if "Error Message" in data:
                logger.error(f"API Error: {data['Error Message']}")
                return None
            
            series_key = f"Time Series FX (Daily)"
            if series_key not in data:
                logger.error(f"Unexpected response format: {data.keys()}")
                return None
                
            # Convert to DataFrame
            time_series = data[series_key]
            df = pd.DataFrame.from_dict(time_series, orient="index")
            
            # Rename columns
            df.rename(columns={
                "1. open": "open",
                "2. high": "high",
                "3. low": "low",
                "4. close": "rate"
            }, inplace=True)
            
            # Convert types
            for col in ["open", "high", "low", "rate"]:
                df[col] = pd.to_numeric(df[col])
            
            # Add currency info and reset index
            df.index = pd.to_datetime(df.index)
            df = df.reset_index()
            df.rename(columns={"index": "timestamp"}, inplace=True)
            df["from_currency"] = from_currency
            df["to_currency"] = to_currency
            df["source"] = "alphavantage"
            
            logger.info(f"Successfully fetched {len(df)} forex records for {from_currency}/{to_currency}")
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for forex {from_currency}/{to_currency}: {str(e)}")
            return None
        except ValueError as e:
            logger.error(f"JSON parsing error for forex {from_currency}/{to_currency}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching forex {from_currency}/{to_currency}: {str(e)}")
            return None
    
    def fetch_economic_indicator(self, indicator: str) -> Optional[pd.DataFrame]:
        """
        Fetch economic indicator data.
        
        Args:
            indicator: Economic indicator code (e.g., 'GDP', 'INFLATION')
            
        Returns:
            DataFrame with economic data or None if error
        """
        # Mapping of friendly names to Alpha Vantage function codes
        indicator_map = {
            "GDP": "REAL_GDP",
            "INFLATION": "INFLATION",
            "UNEMPLOYMENT": "UNEMPLOYMENT",
            "RETAIL_SALES": "RETAIL_SALES",
            "CPI": "CPI"
        }
        
        if indicator not in indicator_map:
            logger.error(f"Unknown economic indicator: {indicator}")
            return None
        
        function = indicator_map[indicator]
        
        try:
            params = {
                "function": function,
                "interval": "quarterly",  # or monthly/annual depending on indicator
                "datatype": "json",
                "apikey": self.api_key
            }
            
            logger.info(f"Fetching economic indicator data for {indicator}")
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API error messages
            if "Error Message" in data:
                logger.error(f"API Error: {data['Error Message']}")
                return None
            
            # Different indicators have different response formats
            if "data" not in data:
                logger.error(f"Unexpected response format: {data.keys()}")
                return None
                
            # Convert to DataFrame
            raw_data = data["data"]
            df = pd.DataFrame(raw_data)
            
            # Common column renaming and processing
            if "value" in df.columns:
                df["value"] = pd.to_numeric(df["value"])
            if "date" in df.columns:
                df["timestamp"] = pd.to_datetime(df["date"])
                df.drop("date", axis=1, inplace=True)
            
            # Add metadata
            df["indicator"] = indicator
            df["country"] = "USA"  # Default for Alpha Vantage
            df["source"] = "alphavantage"
            
            logger.info(f"Successfully fetched {len(df)} records for {indicator}")
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for {indicator}: {str(e)}")
            return None
        except ValueError as e:
            logger.error(f"JSON parsing error for {indicator}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching {indicator}: {str(e)}")
            return None


def fetch_and_store_stock(symbol: str, bronze_layer: str) -> dict:
    """
    Fetch stock data and store in bronze layer.
    
    Args:
        symbol: Stock symbol to fetch
        bronze_layer: Path to bronze layer storage
        
    Returns:
        Dictionary with operation results
    """
    connector = AlphaVantageConnector()
    df = connector.fetch_stock_data(symbol)
    
    if df is None or df.empty:
        return {
            "status": "error",
            "message": f"Failed to fetch data for {symbol}",
            "records_count": 0
        }
    
    # Generate filename and save to bronze layer
    filename = generate_filename("alphavantage", f"stock_{symbol}")
    file_path = save_to_parquet(df, bronze_layer, filename)
    
    return {
        "status": "success",
        "message": f"Successfully fetched and stored data for {symbol}",
        "records_count": len(df),
        "file_path": file_path
    }