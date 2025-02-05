#!/bin/sh
# github-to-sops sops exec-env ../credentials/cloudflare-cf.r2.enc.json ./deploy.sh

 cd "$(dirname "$0")"

export UKC_METRO=fra0

# caching
docker run -d --name buildkitd --privileged moby/buildkit:latest
export KRAFTKIT_BUILDKIT_HOST=docker-container://buildkitd



kraft cloud  deploy -g duckpond -M 2Gi \
            --rollout remove \
            --volume aea0ade2-4f3b-4c2f-9759-958a46f33e92:/root \
            -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
           -e AWS_REGION=$AWS_REGION \
           -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
           -e S3_BUCKET=$S3_BUCKET \
           -e S3_ENDPOINT=$S3_ENDPOINT \
           -e S3_PUBLIC_URL_PREFIX=$S3_PUBLIC_URL_PREFIX \
            ..

tempfile=$(mktemp /tmp/kraft.XXXXXX.json)
trap 'rm -f "$tempfile"' EXIT

kraft cloud instance ls -o json|jq '.[] | select(.service == "duckpond")' > $tempfile

# dont change this pipe cos -o wont work
sops --output-type yaml -e "$tempfile" > deployment.enc.yaml

name=$(sops --output-type json -d deployment.enc.yaml --output-format json |jq .name -r)

kraft cloud  instance logs $name -f