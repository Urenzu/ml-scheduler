import pandas as pd
import random
from datetime import datetime
from data import save_raw, load_latest_raw

def simulate_stream(n=10):
    """Generate synthetic streaming data."""
    data = [{"timestamp": datetime.now(), "value": random.randint(0, 100)} for _ in range(n)]
    return pd.DataFrame(data)

def ingest_batch():
    """Ingest a batch of data into storage."""
    df = simulate_stream()
    # Load latest raw and append
    existing = load_latest_raw()
    combined = pd.concat([existing, df], ignore_index=True)
    path = save_raw(combined)
    print(f"[Ingestion] Saved raw batch to {path}, total records: {len(combined)}")
    return path
