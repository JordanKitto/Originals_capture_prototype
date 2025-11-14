import pandas as pd

def load_csv(path):
    """Load any CSV into a pandas DataFrame."""
    df = pd.read_csv(path, dtype=str)
    print(f"[load_csv] Loaded {len(df)} rows from {path}")
    return df


def get_existing_ids(df):
    """Return a set of DOC_ID values already present."""
    ids = set(df["DOC_ID"].astype(str).tolist())
    print(f"[get_existing_ids] Found {len(ids)} existing DOC_IDs.")
    return ids


def find_new_rows(source_df, existing_ids):
    """Return only the rows with DOC_ID not in existing_ids."""
    source_df = source_df.drop_duplicates(subset=["DOC_ID"], keep="first")
    new_rows = source_df[~source_df["DOC_ID"].astype(str).isin(existing_ids)]
    print(f"[find_new_rows] Found {len(new_rows)} new rows out of {len(source_df)} unique DOC_IDs.")
    return new_rows


def append_new_rows(originals_path, new_rows_df):
    """Append new rows to originals CSV."""
    if new_rows_df.empty:
        print("[append_new_rows] No new rows to append.")
        return
    new_rows_df.to_csv(originals_path, mode="a", header=False, index=False)
    print(f"[append_new_rows] Wrote {len(new_rows_df)} rows to {originals_path}.")
