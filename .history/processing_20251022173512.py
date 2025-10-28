from data import load_latest_raw, save_processed

def deduplicate():
    """Remove duplicate timestamps from raw data."""
    df = load_latest_raw()
    if df.empty:
        print("[Processing] No raw data found.")
        return None
    processed = df.drop_duplicates(subset="timestamp").reset_index(drop=True)
    path = save_processed(processed)
    print(f"[Processing] Deduplicated data saved to {path}, records: {len(processed)}")
    return path

def basic_validation():
    """Check for missing values and numeric ranges."""
    df = load_latest_raw()
    if df.empty:
        return None
    valid = df.dropna(subset=["timestamp", "value"])
    valid = valid[(valid["value"] >= 0) & (valid["value"] <= 100)]
    path = save_processed(valid)
    print(f"[Processing] Validated data saved to {path}, records: {len(valid)}")
    return path
