import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# API configuration
API_HOST = os.getenv("API_HOST", "127.0.0.1")
API_PORT = int(os.getenv("API_PORT", "8000"))
API_WORKERS = int(os.getenv("API_WORKERS", "4"))

# Data source API keys
ALPHAVANTAGE_API_KEY = "HQ6II7W6V47BZAFR"
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "mMmNJzTpMlbtTTHewsWCbrZA7RMPy32d")

# Data lake configuration
DATA_LAKE_PATH = os.getenv("DATA_LAKE_PATH", "data")
BRONZE_LAYER = os.path.join(DATA_LAKE_PATH, "bronze")
SILVER_LAYER = os.path.join(DATA_LAKE_PATH, "silver")
GOLD_LAYER = os.path.join(DATA_LAKE_PATH, "gold")

# Ensure data directories exist
for directory in [BRONZE_LAYER, SILVER_LAYER, GOLD_LAYER]:
    os.makedirs(directory, exist_ok=True)

# Database configuration for metadata
DB_URL = os.getenv("DATABASE_URL", "sqlite:///./financial_metadata.db")