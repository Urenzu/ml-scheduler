import os
import pandas as pd
from datetime import datetime

DATA_DIR = "data"
RAW_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")

# Ensure directories exist
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

# ---- RAW DATA ----
def save_raw(df: pd.DataFrame, version=None):
    """Save raw data with optional versioning."""
    if version is None:
        version = datetime.now().strftime("%Y%m%d%H%M%S")
    path = os.path.join(RAW_DIR, f"raw_{version}.parquet")
    df.to_parquet(path, index=False)
    return path

def load_latest_raw() -> pd.DataFrame:
    """Load the latest raw dataset."""
    files = sorted(os.listdir(RAW_DIR))
    if not files:
        return pd.DataFrame()
    latest = os.path.join(RAW_DIR, files[-1])
    return pd.read_parquet(latest)

# ---- PROCESSED DATA ----
def save_processed(df: pd.DataFrame, version=None):
    """Save processed data with optional versioning."""
    if version is None:
        version = datetime.now().strftime("%Y%m%d%H%M%S")
    path = os.path.join(PROCESSED_DIR, f"processed_{version}.parquet")
    df.to_parquet(path, index=False)
    return path

def load_latest_processed() -> pd.DataFrame:
    """Load the latest processed dataset."""
    files = sorted(os.listdir(PROCESSED_DIR))
    if not files:
        return pd.DataFrame()
    latest = os.path.join(PROCESSED_DIR, files[-1])
    return pd.read_parquet(latest)
