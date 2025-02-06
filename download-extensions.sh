#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <path/to/duckpond binary>"
  exit 1
fi

DUCKPOND="$1"

echo "Collecting DuckDB extension info from $DUCKPOND..."
# Run duckpond to print extension info, then filter json lines that contain an "extension" field.
$DUCKPOND -print-duckdb-extension-info | jq -c 'select(.extension != null)' | while IFS= read -r line; do
    extension=$(echo "$line" | jq -r '.extension')
    ext_url=$(echo "$line" | jq -r '.extension_url')
    dest_path=$(echo "$line" | jq -r '.path')

    echo "Downloading extension '$extension' from $ext_url to $dest_path..."
    mkdir -p "$(dirname "$dest_path")"
    # Remove any existing destination file
    rm -f "$dest_path"

    # Create a temporary file to hold the downloaded gzipped data
    tmp_file=$(mktemp)

    # Download the extension file into the temporary file
    wget -q -O "$tmp_file" "$ext_url"

    # Gunzip the temporary file and write directly to the destination path
    gunzip -c "$tmp_file" > "$dest_path"

    # Remove the temporary file
    rm -f "$tmp_file"
done

echo "All extensions downloaded successfully."
