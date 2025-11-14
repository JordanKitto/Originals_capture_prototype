from src.originals_capture import run_capture

def main():
    source_csv = "data/test_transaction_master.csv"
    originals_csv = "data/originals.csv"

    run_capture(source_csv, originals_csv)

if __name__ == "__main__":
    main()
