#!/bin/bash
set -ex

# Check for exactly 2 arguments: version and commit message
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <commit message>"
  exit 1
fi

VERSION="$1"
COMMIT_MSG="$2"

# Update version in src/version.go using the supplied version
sed -i 's/var Version = ".*"/var Version = "'"$VERSION"'"/' src/version.go || { echo "Error: failed to update src/version.go" >&2; exit 1; }

# Commit changes with provided commit message
git commit -a -m "$COMMIT_MSG"

# Push the commit
git push

# Create a version tag; prefix the version with 'v'
git tag -a "v$VERSION" -m "$COMMIT_MSG"
git push origin "v$VERSION"

# Run GoReleaser to create the release
goreleaser release --draft
