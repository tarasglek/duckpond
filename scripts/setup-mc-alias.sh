#!/bin/bash
#
# setup-mc-alias.sh
# This script configures an mc alias using environment variables.
#
# Required env vars:
#   AWS_ACCESS_KEY_ID
#   AWS_SECRET_ACCESS_KEY
#   S3_ENDPOINT
#
# Optional env vars:
#   AWS_ENDPOINT_URL_IAM
#   AWS_REGION
#   S3_BUCKET
#   S3_PUBLIC_URL_PREFIX

if [ $# -lt 1 ]; then
  echo "Usage: $0 <alias-name>"
  exit 1
fi

ALIAS_NAME="$1"

# Validate required environment variables
if [ -z "${AWS_ACCESS_KEY_ID}" ] || [ -z "${AWS_SECRET_ACCESS_KEY}" ]; then
  echo "Error: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set."
  exit 1
fi

if [ -z "${S3_ENDPOINT}" ]; then
  echo "Error: S3_ENDPOINT must be set."
  exit 1
fi

# Optional variables for logging/verification
if [ -n "${AWS_REGION}" ]; then
  echo "Using AWS_REGION: ${AWS_REGION}"
fi
if [ -n "${AWS_ENDPOINT_URL_IAM}" ]; then
  echo "Using AWS_ENDPOINT_URL_IAM: ${AWS_ENDPOINT_URL_IAM}"
fi
if [ -n "${S3_BUCKET}" ]; then
  echo "Default S3_BUCKET: ${S3_BUCKET}"
fi
if [ -n "${S3_PUBLIC_URL_PREFIX}" ]; then
  echo "Using S3_PUBLIC_URL_PREFIX: ${S3_PUBLIC_URL_PREFIX}"
fi

echo "Setting mc alias '$ALIAS_NAME' with endpoint: ${S3_ENDPOINT}"
mc alias set "${ALIAS_NAME}" "${S3_ENDPOINT}" "${AWS_ACCESS_KEY_ID}" "${AWS_SECRET_ACCESS_KEY}"

if [ $? -eq 0 ]; then
  echo "mc alias '$ALIAS_NAME' set successfully."
else
  echo "Error: Failed to set mc alias '$ALIAS_NAME'."
  exit 1
fi
