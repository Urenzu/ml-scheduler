# main.py
from ml_scheduling.ingestion import DataIngestionSimulator, ingest_csv_file, ingest_json_file
from ml_scheduling.processing import deduplicate, basic_validation
import os

def main():
    print("=== ML Scheduler Demo: Data Pipeline Orchestration ===\n")
    
    # Step 0: create directories
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)

    # Step 1: Ingest data
    print("[Step 1] Generating simulated research data...")
    simulator = DataIngestionSimulator()
    raw_df = simulator.generate_research_data_batch(num_records=100)
    
    raw_path = "data/raw/research_batch.csv"
    raw_df.to_csv(raw_path, index=False)
    print(f"[Step 1] Raw data saved at: {raw_path}\n")

    # Step 2: Deduplicate
    print("[Step 2] Deduplicating data...")
    dedup_path = deduplicate(raw_path)
    print(f"[Step 2] Deduplicated data saved at: {dedup_path}\n")

    # Step 3: Basic validation
    print("[Step 3] Validating data...")
    validated_path = basic_validation(dedup_path)
    print(f"[Step 3] Validated data saved at: {validated_path}\n")

    print("=== Data Pipeline Complete. Dataset ready for scheduling/jobs ===")

if __name__ == "__main__":
    main()
