#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <path/to/duckpond binary>"
  exit 1
fi

DUCKPOND="$1"

# Create a temporary file for the JSON output
tmp_json=$(mktemp)
# Set a trap to remove the temporary JSON file on exit
trap "rm -f '${tmp_json}'" EXIT

echo "Collecting DuckDB extension info from $DUCKPOND into ${tmp_json}..."
$DUCKPOND -print-duckdb-extension-info > "${tmp_json}"

# Process only valid JSON lines from the temp file
grep -E '^\{' "${tmp_json}" | jq -c 'select(.extension != null)' | while IFS= read -r line; do
    extension=$(echo "$line" | jq -r '.extension')
    ext_url=$(echo "$line" | jq -r '.extension_url')
    dest_path=$(echo "$line" | jq -r '.path')

    echo "Downloading extension '$extension' from $ext_url to $dest_path..."
    mkdir -p "$(dirname "$dest_path")"
    # Remove any existing destination file
    rm -f "$dest_path"

    # Create a temporary file to hold the downloaded gzipped data
    tmp_file=$(mktemp)
    # Set a trap (scoped to this loop iteration) to remove the temporary file on exit
    trap "rm -f '${tmp_file}'" EXIT

    # Download the extension file into the temporary file
    wget -q -O "$tmp_file" "$ext_url"

    # Gunzip the temporary file and write directly to the destination path
    gunzip -c "$tmp_file" > "$dest_path"

    # Disable the trap for this iteration
    trap - EXIT
done

# Remove the temporary JSON file explicitly (if the trap hasn't done it already)
rm -f "${tmp_json}"
trap - EXIT

echo "All extensions downloaded successfully."
