from src.originals_capture import run_capture

def main():
    source_csv = "data/transaction_master_Copy.csv"
    originals_csv = "data/Original_Invoice_Data_CSV_Copy.csv"

    run_capture(source_csv, originals_csv, days_back=30)

if __name__ == "__main__":
    main()
