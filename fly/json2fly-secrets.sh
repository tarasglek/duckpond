
#!/bin/bash

if [ -t 0 ]; then
    echo "Usage: cat secrets.json | json2fly-secrets.sh [flags]" >&2
    echo "Reads a JSON object from stdin and converts it to a flyctl secrets set invocation." >&2
    exit 1
fi
# Parse the JSON from stdin into NAME=VALUE pairs. Note that we force values to strings.
secrets=$(jq -r 'to_entries | map("\(.key)=\(.value|@json)") | join(" ")')
if [ -z "$secrets" ]; then
    echo "Error: No secrets found in input" >&2
    exit 1
fi

# Output the complete flyctl secrets set command
eval flyctl secrets set "$@" $secrets
