
#!/bin/bash

if [ -t 0 ]; then
    echo "Usage: cat secrets.json | json2fly-secrets.sh [flags]" >&2
    echo "Reads a JSON object from stdin and converts it to a flyctl secrets set invocation." >&2
    exit 1
fi
# Read the entire input into a variable
input=$(cat)

# Extract secrets as NAME=VALUE pairs (forcing values to strings)
secrets=$(echo "$input" | jq -r 'to_entries | map("\(.key)=\(.value|@json)") | join(" ")')
if [ -z "$secrets" ]; then
    echo "Error: No secrets found in input" >&2
    exit 1
fi

# Output the complete flyctl secrets set command
eval flyctl secrets set "$@" $secrets

# Extract and output the secret keys that were set
keys=$(echo "$input" | jq -r 'keys[]')
echo "Secrets set:" $keys
