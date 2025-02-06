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
    wget -q -O "$dest_path" "$ext_url"
done

echo "All extensions downloaded successfully."
