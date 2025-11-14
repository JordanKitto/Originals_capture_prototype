# Originals Capture Prototype

This prototype demonstrates the logic behind capturing *original* invoice data from a daily transaction extract.

The goal is simple:
- Each day, a transaction export (test_transaction_master.csv) arrives.
- A master file (originals.csv) stores the *first time* each DOC_ID was ever seen.
- This script appends only *new* DOC_IDs, preventing duplicates.

## Features
- Loads both files using pandas
- Extracts unique DOC_IDs
- Identifies new DOC_IDs since last run
- Appends only the new rows to `originals.csv`
- Safe to run multiple times (idempotent)

## How To Run

