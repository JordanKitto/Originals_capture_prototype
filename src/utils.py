import os
import pandas as pd
from datetime import datetime, timedelta

ORIGINALS_COLUMNS = [
    "DOC_ID",
    "INVOICE_TYPE",
    "ENTRY_DATE",
    "COMPANY_CODE",
    "DOC_DATE",
    "INVOICE_NUMBER",
    "AMOUNT",
    "VENDOR_NUM",
    "VENDOR_NAME_1",
    "VENDOR_NAME_2",
    "PO_NUM",
    "ABN",
    "DSS_DOWNLOAD_DATE",
    "STATUS_TEXT",
]

def filter_recent_by_entry_date(df: pd.DataFrame, days: int = 30) -> pd.DataFrame:
    """
    Filter the DataFrame to only include rows where ENTRY_DATE is within the last 
    30 days based on the current date.

    Assumes ENTRY_DATE is in 'YYYY-MM-DD' format.
    Any rows with invalid or missing ENTRY_DATE will be excluded.
    """

    if "ENTRY_DATE" not in df.columns:
        raise ValueError("[filter_recent_by_entry_date] ENTRY_DATE column not found in DataFrame.")
    if df.empty:
        print("[filter_recent_by_entry_date] DataFrame is empty. No rows to filter.")
        return df.copy()
    
    df = df.copy()
    # Handle Australian date format if needed
    df["__ENTRY_DATE"] = pd.to_datetime(
        df["ENTRY_DATE"],
        errors="coerce",
        dayfirst=True
    )

    before = len(df)
    today = datetime.now().date()
    cutoff = today - timedelta(days=days)

    recent = df[df["__ENTRY_DATE"].dt.date >= cutoff].drop(columns="__ENTRY_DATE")
    after = len(recent)

    dropped = before - after
    print(
        f"[filter_recent_by_entry_date] kept {after} rows out of {before} "
        f"using ENTRY_DATE >= {cutoff} (dropped {dropped} rows)."
    )
    return recent

def load_csv(path: str) -> pd.DataFrame:
    """Load any CSV into a pandas DataFrame as strings."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"[load_csv] File not found: {path}")
    df = pd.read_csv(path, dtype=str)
    print(f"[load_csv] Loaded {len(df)} rows from {path}")
    return df


def to_originals_schema(source_df: pd.DataFrame) -> pd.DataFrame:
    """
    Given a full Transaction Master DataFrame, trim it to the originals schema
    and enforce column order.

    This will:
    - check that all required columns exist
    - return a new DataFrame with exactly ORIGINALS_COLUMNS in that order
    """
    missing = [c for c in ORIGINALS_COLUMNS if c not in source_df.columns]
    if missing:
        raise ValueError(
            f"[to_originals_schema] Source is missing required columns for originals schema: {missing}"
        )

    trimmed = source_df[ORIGINALS_COLUMNS].copy()
    print(
        f"[to_originals_schema] Trimmed source to originals schema with "
        f"{len(trimmed)} rows and {len(trimmed.columns)} columns."
    )
    return trimmed


def get_existing_ids(df: pd.DataFrame) -> set:
    """
    Return a set of DOC_ID values already present in the originals DataFrame.
    If DOC_ID is missing or df is empty, return an empty set.
    """
    if df.empty:
        print("[get_existing_ids] Originals DataFrame is empty, no existing DOC_IDs.")
        return set()

    if "DOC_ID" not in df.columns:
        print("[get_existing_ids] DOC_ID column not found in originals. Returning empty set.")
        return set()

    ids = set(df["DOC_ID"].astype(str).tolist())
    print(f"[get_existing_ids] Found {len(ids)} existing DOC_IDs.")
    return ids


def find_new_rows(source_df: pd.DataFrame, existing_ids: set) -> pd.DataFrame:
    """
    From the source DataFrame (already trimmed to originals schema),
    return only rows with DOC_ID not in existing_ids.

    Also drops duplicate DOC_IDs inside the source file itself.
    """
    if source_df.empty:
        print("[find_new_rows] Source DataFrame is empty. No new rows.")
        return source_df.copy()

    if "DOC_ID" not in source_df.columns:
        raise ValueError("[find_new_rows] Column DOC_ID not found in source DataFrame.")

    df = source_df.copy()
    df["DOC_ID"] = df["DOC_ID"].astype(str)

    # Drop duplicate DOC_IDs within the source file
    before = len(df)
    df = df.drop_duplicates(subset=["DOC_ID"], keep="first")
    after = len(df)
    if after != before:
        print(f"[find_new_rows] Dropped {before - after} duplicate DOC_IDs in source file.")

    # Keep only rows that are not already in originals
    mask_new = ~df["DOC_ID"].isin(existing_ids)
    new_rows = df[mask_new].copy()
    print(
        f"[find_new_rows] Found {len(new_rows)} new rows out of {len(df)} unique DOC_IDs in source."
    )
    return new_rows


def append_new_rows(originals_path: str, new_rows_df: pd.DataFrame) -> int:
    """
    Append new rows to originals CSV on disk, enforcing the originals schema.

    Returns the number of rows written.
    """
    if new_rows_df.empty:
        print("[append_new_rows] No new rows to append.")
        return 0

    # Force columns to match originals schema and order
    missing = [c for c in ORIGINALS_COLUMNS if c not in new_rows_df.columns]
    if missing:
        raise ValueError(
            f"[append_new_rows] New rows DataFrame is missing required columns: {missing}"
        )

    trimmed = new_rows_df[ORIGINALS_COLUMNS].copy()

    # Decide whether we need to write the header
    header_needed = not os.path.exists(originals_path) or os.path.getsize(originals_path) == 0

    trimmed.to_csv(
        originals_path,
        mode="a",
        index=False,
        header=header_needed,
    )

    print(f"[append_new_rows] Wrote {len(trimmed)} rows to {originals_path}.")
    return len(trimmed)
