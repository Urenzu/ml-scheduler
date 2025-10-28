# processing.py
import pandas as pd
import os

PROCESSED_DIR = "data/processed"

def deduplicate(input_path: str) -> str:
    """Remove duplicate IDs or timestamps from a CSV file."""
    if not os.path.exists(input_path):
        print(f"[Processing] File not found: {input_path}")
        return None

    df = pd.read_csv(input_path)
    if df.empty:
        print("[Processing] No data found in file.")
        return None

    # Remove duplicates based on 'id' column if exists, else all columns
    subset = ["id"] if "id" in df.columns else None
    deduped = df.drop_duplicates(subset=subset).reset_index(drop=True)

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    output_path = os.path.join(PROCESSED_DIR, f"dedup_{os.path.basename(input_path)}")
    deduped.to_csv(output_path, index=False)

    print(f"[Processing] Deduplicated data saved to {output_path}, records: {len(deduped)}")
    return output_path


def basic_validation(input_path: str) -> str:
    """Simple validation: drop rows with missing values and filter numeric ranges."""
    if not os.path.exists(input_path):
        print(f"[Processing] File not found: {input_path}")
        return None

    df = pd.read_csv(input_path)
    if df.empty:
        return None

    # Drop rows with missing values
    valid = df.dropna()

    # Apply simple numeric validation if 'citation_count' or 'value' exists
    if "citation_count" in valid.columns:
        valid = valid[(valid["citation_count"] >= 0) & (valid["citation_count"] <= 1000)]
    if "value" in valid.columns:
        valid = valid[(valid["value"] >= 0) & (valid["value"] <= 1000)]

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    output_path = os.path.join(PROCESSED_DIR, f"validated_{os.path.basename(input_path)}")
    valid.to_csv(output_path, index=False)

    print(f"[Processing] Validated data saved to {output_path}, records: {len(valid)}")
    return output_path
