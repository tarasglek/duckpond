import deltalake as dl
import pandas as pd
from datetime import datetime, timedelta

# Path to the Delta Lake table
path = "delta_table/"

# Function to generate data with primary key and last-modified timestamp
def generate_data(start_id, num_rows, start_time):
    return pd.DataFrame({
        "id": range(start_id, start_id + num_rows),
        "message": ["Hello, World!"] * num_rows,
        "last_modified": [
            start_time - timedelta(days=i) for i in range(num_rows)
        ],
        "user": [
            "even_user" if (start_id + i) % 2 == 0 else "odd_user" 
            for i in range(num_rows)
        ]
    })

# Generate first batch of data
batch1 = generate_data(start_id=1, num_rows=3, start_time=datetime.now())

# Write first batch to Delta Lake
dl.write_deltalake(path, batch1, mode="append")

# Generate second batch of data
batch2 = generate_data(start_id=4, num_rows=3, start_time=datetime.now())

# Write second batch to Delta Lake
dl.write_deltalake(path, batch2, mode="append")

print(f"Data written to Delta Lake at '{path}' in two batches successfully!")
