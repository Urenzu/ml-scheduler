# main.py
from ml_scheduling.ingestion import DataIngestionSimulator
from ml_scheduling.processing import deduplicate, basic_validation
import os
import pandas as pd

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

def save_batch(df: pd.DataFrame, filename: str) -> str:
    """Save a DataFrame to a CSV file in RAW_DIR."""
    os.makedirs(RAW_DIR, exist_ok=True)
    path = os.path.join(RAW_DIR, filename)
    df.to_csv(path, index=False)
    return path


def main():
    print("=== ML Scheduler Demo: Data Pipeline Orchestration ===\n")

    simulator = DataIngestionSimulator()

    # -----------------------------
    # Batch ingestion
    # -----------------------------
    print("[Step 1] Batch ingestion...")
    batch_df = simulator.generate_research_data_batch(num_records=100)
    batch_path = save_batch(batch_df, "batch_research.csv")
    print(f"[Step 1] Batch data saved at: {batch_path}\n")

    # -----------------------------
    # Streaming ingestion (optional)
    # -----------------------------
    print("[Step 1b] Streaming ingestion (simulated)...")
    for i, stream_df in enumerate(simulator.stream_research_data(batch_size=20, num_batches=3, delay_seconds=0.5)):
        path = save_batch(stream_df, f"stream_batch_{i+1}.csv")
        print(f"[Step 1b] Stream batch {i+1} saved at: {path}")

    # -----------------------------
    # Deduplication
    # -----------------------------
    print("\n[Step 2] Deduplicating batch data...")
    dedup_path = deduplicate(batch_path)
    print(f"[Step 2] Deduplicated data saved at: {dedup_path}\n")

    # -----------------------------
    # Validation
    # -----------------------------
    print("[Step 3] Validating data...")
    validated_path = basic_validation(dedup_path)
    print(f"[Step 3] Validated data saved at: {validated_path}\n")

    print("=== Data Pipeline Complete. Dataset ready for scheduling/jobs ===")


if __name__ == "__main__":
    main()
