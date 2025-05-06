import requests
import pandas as pd
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import time
import random
from utils.helpers import generate_filename, save_to_parquet

logger = logging.getLogger(__name__)


class YahooFinanceConnector:
    """
    Connector for Yahoo Finance API with improved rate limiting handling.
    Using the unofficial API.
    """

    def __init__(self, request_delay: float = 2.0):
        self.base_url = "https://query1.finance.yahoo.com/v8/finance/chart/"
        self.request_delay = request_delay  # Minimum delay between requests in seconds
        self.last_request_time = 0  # Track when the last request was made
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:97.0) Gecko/20100101 Firefox/97.0"
        ]

    def _get_random_user_agent(self) -> str:
        """Return a random user agent string."""
        return random.choice(self.user_agents)

    def _throttle_request(self):
        """Ensure minimum delay between requests to avoid rate limiting."""
        current_time = time.time()
        elapsed = current_time - self.last_request_time

        if elapsed < self.request_delay:
            # Add some randomization to the delay to avoid detection patterns
            sleep_time = self.request_delay - elapsed + random.uniform(0.1, 1.0)
            logger.debug(f"Throttling request, sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def _fetch_data_with_retry(self, url: str, params: dict, max_retries: int = 5) -> Optional[dict]:
        """Helper function to fetch data with retries and improved backoff strategy."""
        headers = {
            "User-Agent": self._get_random_user_agent(),
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://finance.yahoo.com",
            "Referer": "https://finance.yahoo.com/",
        }

        for attempt in range(max_retries):
            self._throttle_request()  # Apply throttling before each request

            try:
                logger.info(f"Fetching data from Yahoo Finance (attempt {attempt + 1}/{max_retries})")
                response = requests.get(url, params=params, headers=headers, timeout=10)

                # Check for rate limiting or other errors
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    # Rate limited - use exponential backoff with jitter
                    wait_time = min(30, (2 ** attempt) * (1 + random.random()))
                    logger.warning(f"Rate limit exceeded. Waiting {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                else:
                    response.raise_for_status()  # Raise HTTPError for other bad responses
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error (attempt {attempt + 1}/{max_retries}): {e}")

                # If we're on the last attempt, return None
                if attempt == max_retries - 1:
                    logger.error(f"Maximum retries reached for {url}. Giving up.")
                    return None

                # Otherwise wait and retry with longer delay for non-429 errors
                wait_time = min(30, (2 ** attempt) + random.uniform(1, 3))
                logger.warning(f"Request failed. Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)

        return None

    def fetch_stock_data(self, symbol: str, period1: int = None, period2: int = None,
                         interval: str = "1d") -> Optional[pd.DataFrame]:
        """
        Fetch stock data from Yahoo Finance.

        Args:
            symbol: Stock ticker symbol
            period1: Start timestamp (Unix time)
            period2: End timestamp (Unix time)
            interval: Data interval - 1d, 1wk, 1mo

        Returns:
            DataFrame with stock data or None if error
        """
        # Default time period: last 2 years if not specified
        if period1 is None:
            period1 = int((datetime.now() - timedelta(days=730)).timestamp())
        if period2 is None:
            period2 = int(datetime.now().timestamp())

        params = {
            "period1": period1,
            "period2": period2,
            "interval": interval,
            "includePrePost": "false",
            "events": "div,split"
        }

        url = f"{self.base_url}{symbol}"
        logger.info(f"Fetching Yahoo Finance data for {symbol}")

        data = self._fetch_data_with_retry(url, params)
        if data is None:
            return None

        try:
            # Check if we got valid data
            if "chart" not in data or "result" not in data["chart"] or not data["chart"]["result"]:
                logger.error(f"Invalid data format for {symbol}")
                return None

            chart_data = data["chart"]["result"][0]

            # Extract time series data
            timestamps = chart_data["timestamp"]
            quote = chart_data["indicators"]["quote"][0]

            # Create DataFrame
            df = pd.DataFrame({
                "timestamp": [datetime.fromtimestamp(ts) for ts in timestamps],
                "open": quote.get("open", [None] * len(timestamps)),
                "high": quote.get("high", [None] * len(timestamps)),
                "low": quote.get("low", [None] * len(timestamps)),
                "close": quote.get("close", [None] * len(timestamps)),
                "volume": quote.get("volume", [None] * len(timestamps)),
                "symbol": symbol,
                "source": "yahoo_finance"
            })

            # Drop rows with missing values
            df = df.dropna()

            logger.info(f"Successfully fetched {len(df)} records for {symbol}")
            return df
        except (ValueError, KeyError, IndexError) as e:
            logger.error(f"Error processing Yahoo Finance data: {e}")
            return None

    def fetch_crypto_data(self, symbol: str, period1: int = None, period2: int = None,
                          interval: str = "1d") -> Optional[pd.DataFrame]:
        """
        Fetch cryptocurrency data from Yahoo Finance.

        Args:
            symbol: Crypto symbol (e.g., 'BTC-USD')
            period1: Start timestamp (Unix time)
            period2: End timestamp (Unix time)
            interval: Data interval - 1d, 1wk, 1mo

        Returns:
            DataFrame with crypto data or None if error
        """
        # Yahoo Finance uses the same endpoint for crypto as for stocks
        # But we add the crypto suffix if not present
        if "-USD" not in symbol and not symbol.endswith("USDT"):
            symbol = f"{symbol}-USD"

        # Use the stock data function and add crypto-specific transformations
        df = self.fetch_stock_data(symbol, period1, period2, interval)

        if df is not None:
            # Rename for consistency with crypto terminology
            df = df.rename(columns={"close": "price"})
            df["data_type"] = "crypto"

            # Extract the base symbol without the -USD suffix
            df["base_symbol"] = symbol.split("-")[0]

            return df

        return None


def fetch_and_store_stock_yahoo(symbol: str, bronze_layer: str) -> dict:
    """
    Fetch stock data from Yahoo Finance and store in bronze layer.

    Args:
        symbol: Stock symbol to fetch
        bronze_layer: Path to bronze layer storage

    Returns:
        Dictionary with operation results
    """
    connector = YahooFinanceConnector(request_delay=3.0)  # Use a 3-second minimum delay
    df = connector.fetch_stock_data(symbol)

    if df is None or df.empty:
        return {
            "status": "error",
            "message": f"Failed to fetch Yahoo Finance data for {symbol}",
            "records_count": 0
        }

    filename = generate_filename("yahoo_finance", f"stock_{symbol}")
    file_path = save_to_parquet(df, bronze_layer, filename)

    return {
        "status": "success",
        "message": f"Successfully fetched and stored Yahoo Finance data for {symbol}",
        "records_count": len(df),
        "file_path": file_path
    }


def batch_fetch_symbols(symbols: List[str], bronze_layer: str, batch_delay: float = 5.0) -> Dict[str, Any]:
    """
    Fetch multiple symbols with delays between each to avoid rate limiting.

    Args:
        symbols: List of symbols to fetch
        bronze_layer: Path to bronze layer storage
        batch_delay: Delay between symbol fetches in seconds

    Returns:
        Dictionary with results for each symbol
    """
    results = {}

    for i, symbol in enumerate(symbols):
        logger.info(f"Processing symbol {i + 1}/{len(symbols)}: {symbol}")

        # Add extra delay between batches
        if i > 0:
            delay = batch_delay + random.uniform(1, 3)
            logger.info(f"Waiting {delay:.2f} seconds before next symbol...")
            time.sleep(delay)

        result = fetch_and_store_stock_yahoo(symbol, bronze_layer)
        results[symbol] = result

    return results