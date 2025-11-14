import os
from src.utils import load_csv, get_existing_ids, find_new_rows, append_new_rows


def run_capture(source_csv, originals_csv):
    """Perform originals capture: detect and append new DOC_ID rows."""
    source_df = load_csv(source_csv)
    originals_df = load_csv(originals_csv)

    existing_ids = get_existing_ids(originals_df)
    new_rows = find_new_rows(source_df, existing_ids)

    print("\n[run_capture] New rows that will be appended:")
    print(new_rows[["DOC_ID"]])

    append_new_rows(originals_csv, new_rows)

    print("\n[run_capture] Capture complete.")


if __name__ == "__main__":
    source_csv = "data/test_transaction_master.csv"
    originals_csv = "data/originals.csv"
    run_capture(source_csv, originals_csv)
