#!/bin/sh
# github-to-sops sops exec-env ../credentials/cloudflare-cf.r2.enc.json ./deploy.sh
export UKC_METRO=fra0
kraft cloud  deploy -g duckpond -M 2Gi \
            --rollout remove \
            -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
           -e AWS_REGION=$AWS_REGION \
           -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
           -e S3_BUCKET=$S3_BUCKET \
           -e S3_ENDPOINT=$S3_ENDPOINT \
           -e S3_PUBLIC_URL_PREFIX=$S3_PUBLIC_URL_PREFIX \
            ..