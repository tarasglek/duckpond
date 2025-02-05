#!/bin/sh
# github-to-sops sops exec-env ../credentials/cloudflare-cf.r2.enc.json ./deploy.sh
export UKC_METRO=fra0

docker run -d --name buildkitd --privileged moby/buildkit:latest
export KRAFTKIT_BUILDKIT_HOST=docker-container://buildkitd

kraft cloud  deploy -g duckpond -M 2Gi \
            --rollout remove \
            -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
           -e AWS_REGION=$AWS_REGION \
           -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
           -e S3_BUCKET=$S3_BUCKET \
           -e S3_ENDPOINT=$S3_ENDPOINT \
           -e S3_PUBLIC_URL_PREFIX=$S3_PUBLIC_URL_PREFIX \
            .. \
            -o json > /tmp/tempfile.json

sops --output-type yaml -e /tmp/tempfile.json  -o > deployment.enc.yaml
