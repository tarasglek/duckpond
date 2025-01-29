import deltalake as dl
import pandas as pd
from datetime import datetime, timedelta
import shutil
import os

# Set storage options
storage_options = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "AWS_ENDPOINT_URL": os.getenv("S3_ENDPOINT"),
} if os.getenv("AWS_ACCESS_KEY_ID") else None

# Set path based on storage type
path = f"s3://{os.getenv('S3_BUCKET', '')}/delta_table/" if storage_options else "delta_table/"

# Reset local database if not using S3
if not storage_options:
    shutil.rmtree(path, ignore_errors=True)

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

# Helper function for writing data
def write_data(data, mode):
    dl.write_deltalake(
        path, 
        data, 
        mode=mode, 
        partition_by=["user"],
        storage_options=storage_options
    )

# Generate and write data
batch1 = generate_data(start_id=1, num_rows=3, start_time=datetime.now())
write_data(batch1, "append")

batch2 = generate_data(start_id=4, num_rows=3, start_time=datetime.now())
write_data(batch2, "append")

# delete_data = pd.DataFrame({"id": [1], "message": [None], "last_modified": [None], "user": [None]})
# write_data(delete_data, "overwrite")

print(f"Data written to Delta Lake at '{path}' in two batches successfully!")
