import os
import time
import pandas as pd
from datetime import datetime

input_file = "dataset/patients_data_with_alerts.xlsx"

# Watched directory
stream_directory = "stream_input"

# Using 10,000 rows as per the assignment instructions
rows = 10000

# Number of rows in each streaming batch file
rows_per_batch = 500

# Delay between writing each batch file
sleep_seconds = 5

# Clearing old CSV files from stream_input before writing new batches
for file_name in os.listdir(stream_directory):
    file_path = os.path.join(stream_directory, file_name)

    if file_name.endswith(".csv") and os.path.isfile(file_path):
        os.remove(file_path)

df = pd.read_excel(input_file)

print("Dataset columns:")
print(df.columns.tolist())

# Preprocessing the dataset for stream processing
df = df[["Patient Number", "Heart Rate (bpm)"]].copy()
df["patient_number"] = pd.to_numeric(df["Patient Number"], errors="coerce")
df["heart_rate"] = pd.to_numeric(df["Heart Rate (bpm)"], errors="coerce")
df = df[["patient_number", "heart_rate"]].copy()

# Removing rows where patient_number or heart_rate is missing
df = df.dropna(subset=["patient_number", "heart_rate"])

df["patient_number"] = df["patient_number"].astype(int)
df = df.head(rows)

# Creating timestamps since the original dataset does not have timestamp column
df["timestamp"] = pd.date_range(
    start="2026-01-01 10:00:00",
    periods=len(df),
    freq="10s"
)

df = df[["timestamp", "patient_number", "heart_rate"]]

# Creating alert case for two consecutive 2-minute windows
alert_rows = pd.DataFrame([
    {
        "timestamp": datetime(2026, 1, 1, 10, 5, 10),
        "patient_number": 999999,
        "heart_rate": 125
    },
    {
        "timestamp": datetime(2026, 1, 1, 10, 5, 50),
        "patient_number": 999999,
        "heart_rate": 132
    },
    {
        "timestamp": datetime(2026, 1, 1, 10, 7, 10),
        "patient_number": 999999,
        "heart_rate": 128
    },
    {
        "timestamp": datetime(2026, 1, 1, 10, 7, 50),
        "patient_number": 999999,
        "heart_rate": 135
    },
])

df = pd.concat([df, alert_rows], ignore_index=True)
df = df.sort_values("timestamp")

print(f"Total rows used: {len(df)}")
print("Streaming simulation starting...")

# Writing small CSV files into stream_input one by one
for batch_number, start_row in enumerate(range(0, len(df), rows_per_batch), start=1):
    batch = df.iloc[start_row:start_row + rows_per_batch]

    output_file = f"{stream_directory}/batch_{batch_number}.csv"

    batch.to_csv(output_file, index=False)

    print(f"Wrote {output_file} with {len(batch)} rows")

    # Wait before writing the next batch
    time.sleep(sleep_seconds)

print("Streaming file writing complete.")