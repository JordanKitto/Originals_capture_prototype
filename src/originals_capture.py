from src.utils import (
    load_csv,
    to_originals_schema,
    get_existing_ids,
    find_new_rows,
    append_new_rows,
    filter_recent_by_entry_date,
)


def run_capture(source_csv: str, originals_csv: str, days_back: int = 30) -> None:
    """
    Run the originals capture process:

    1. Load the full Transaction Master CSV (source_csv).
    2. Trim it to the originals schema.
    3. Load the existing originals CSV (originals_csv).
    4. Work out which DOC_IDs are new.
    5. Append only those new rows to originals_csv.
    6. Append only rows with ENTRY_DATE in the last 30 days.
    """
  # 1. Load the full Transaction Master export
    src_full = load_csv(source_csv)

    # 2. Trim to originals schema (only the columns you care about)
    src_view = to_originals_schema(src_full)

    # 3. Filter down to recent rows only, based on ENTRY_DATE
    src_recent = filter_recent_by_entry_date(src_view, days=days_back)

    if src_recent.empty:
        print("[run_capture] No recent rows in source after ENTRY_DATE filter. Nothing to do.")
        return

    # 4. Load current originals
    orig_df = load_csv(originals_csv)

    # 5. Determine which DOC_IDs already exist (using normalised keys)
    existing_ids = get_existing_ids(orig_df)

    # 6. Find new rows in the recent slice
    new_rows = find_new_rows(src_recent, existing_ids)

    total_new = len(new_rows)
    print(f"\n[run_capture] New DOC_IDs that will be appended: {total_new}")

    if total_new > 0:
        sample = new_rows["DOC_ID"].head(10)
        print("[run_capture] Sample of first 10 DOC_IDs:")
        print(sample.to_string(index=False))

    # 7. Append new rows
    written = append_new_rows(originals_csv, new_rows)

    print(f"\n[run_capture] Capture complete. Rows written: {written}")
