from ingestion import ingest_batch
from processing import deduplicate, basic_validation

def main():
    print("=== ML Scheduler Demo: Data Pipeline Orchestration ===\n")

    # Step 1: Ingest data
    print("[Step 1] Ingesting data...")
    raw_path = ingest_batch()
    print(f"[Step 1] Raw data saved at: {raw_path}\n")

    # Step 2: Deduplicate
    print("[Step 2] Deduplicating data...")
    dedup_path = deduplicate()
    if dedup_path:
        print(f"[Step 2] Deduplicated data saved at: {dedup_path}\n")

    # Step 3: Basic validation
    print("[Step 3] Validating data...")
    validated_path = basic_validation()
    if validated_path:
        print(f"[Step 3] Validated data saved at: {validated_path}\n")

    print("=== Data Pipeline Complete. Dataset ready for scheduling/jobs ===")

if __name__ == "__main__":
    main()
