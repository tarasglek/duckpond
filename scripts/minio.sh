#!/bin/bash
# minio-start.sh

# Get script directory (works with symlinks too)
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"

# Inline configuration
MINIO_ROOT_USER=${MINIO_ROOT_USER:-demo}
MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-demo-pass}
MINIO_API_PORT=${MINIO_API_PORT:-8883}
S3_BUCKET=${S3_BUCKET:-duckpond}  # Default bucket name
MINIO_CONSOLE_PORT=${MINIO_CONSOLE_PORT:-$((MINIO_API_PORT + 1))}
MINIO_HOST=${MINIO_HOST:-localhost}
MINIO_DATA_DIR=${MINIO_DATA_DIR:-"$SCRIPT_DIR/../src/testdata/minio"}

# Export for MinIO server to use
export MINIO_ROOT_USER
export MINIO_ROOT_PASSWORD

# Ensure data directory exists
mkdir -p "$MINIO_DATA_DIR"

# Function to start MinIO server
start_server() {
    echo "Starting MinIO Server..."
    echo "Data directory: $MINIO_DATA_DIR"
    echo "API Port: $MINIO_API_PORT"
    echo "Console Port: $MINIO_CONSOLE_PORT"

    # Start trace logging concurrently
    (
      trap "exit" INT
      sleep 1
      while true; do
        mc admin trace --call s3 s3 --no-color || true
        sleep 1
      done
    ) &
    TRACE_PID=$!
    trap "kill $TRACE_PID; exit" INT

    minio server "$MINIO_DATA_DIR" \
        --address ":${MINIO_API_PORT}" \
        --console-address ":${MINIO_CONSOLE_PORT}" \
        "$@"
}

# Function to configure MinIO client
setup_client() {
    echo "Configuring MinIO Client..."
    mc alias set s3 \
        "http://${MINIO_HOST}:${MINIO_API_PORT}" \
        "$MINIO_ROOT_USER" \
        "$MINIO_ROOT_PASSWORD"

    # Create default bucket if it doesn't exist
    if ! mc stat s3/"$S3_BUCKET" &>/dev/null; then
        echo "Creating default bucket: $S3_BUCKET"
        mc mb s3/"$S3_BUCKET"
    fi

    # Pass remaining arguments to mc if any
    if [ $# -gt 0 ]; then
        mc "$@"
    fi
}

# Main execution
case "$1" in
    "server")
        shift  # Remove 'server' from args
        start_server "$@"
        ;;
    "client")
        shift  # Remove 'client' from args
        setup_client "$@"
        ;;
    "all")
        shift  # Remove 'all' from args
        start_server "$@"
        setup_client "$@"
        ;;
    *)
        if [ $# -eq 0 ] || [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
            echo "MinIO Management Script"
            echo
            echo "Usage: $0 {server|client|all} [additional arguments...]"
            echo
            echo "Standard Options:"
            echo "  --help, -h       Show this help message"
            echo "  --version        Show version information"
            echo
            echo "Server Options:"
            minio server --help | sed -n '/OPTIONS:/,$p'
            echo
            echo "Client Options:"
            mc --help | sed -n '/OPTIONS:/,$p'
            echo
            echo "Current configuration:"
            echo "MINIO_ROOT_USER=$MINIO_ROOT_USER"
            echo "MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD"
            echo "MINIO_API_PORT=$MINIO_API_PORT"
            echo "MINIO_CONSOLE_PORT=$MINIO_CONSOLE_PORT"
            echo "MINIO_HOST=$MINIO_HOST"
            echo "MINIO_DATA_DIR=$MINIO_DATA_DIR"
            echo "S3_BUCKET=$S3_BUCKET"
            exit 0
        elif [ "$1" = "--version" ]; then
            echo "MinIO Server:"
            minio --version
            echo
            echo "MinIO Client:"
            mc --version
            exit 0
        else
            echo "Invalid command: $1"
            echo "Use --help for usage information"
            exit 1
        fi
        ;;
esac
