#docker build -t duckpond .
FROM golang:1.23-bookworm AS build

WORKDIR /src

COPY ./src/go.mod /src/go.mod
COPY ./src/go.sum /src/go.sum
RUN go mod download

COPY ./src /src

RUN set -xe; \
    go build \
      -buildmode=pie \
      -ldflags "-linkmode external -extldflags -static-pie" \
      -tags netgo \
      -o /duckpond /src/... \
    ;

FROM debian:bookworm-slim

# Install dependencies for extension download script
RUN apt-get update && apt-get install -y \
    jq \
    wget \
    && rm -rf /var/lib/apt/lists/*

# /root includes .duckdb/extensions
COPY --from=build /root /root 
COPY --from=build /duckpond /duckpond
COPY download-extensions.sh /download-extensions.sh
RUN chmod +x /download-extensions.sh && /download-extensions.sh /duckpond

ENTRYPOINT [ "/duckpond", "-port", "8080" ]
