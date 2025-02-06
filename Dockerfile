#docker build -t duckpond .
FROM golang:1.23-bookworm AS build

WORKDIR /src

COPY ./src/go.mod /src/go.mod
COPY ./src/go.sum /src/go.sum
RUN go mod download

COPY ./src /src

RUN wget https://github.com/duckdb/duckdb/releases/download/v1.1.3/libduckdb-linux-amd64.zip -O /tmp/libduckdb.zip && \
    unzip /tmp/libduckdb.zip -d /tmp/libduckdb && \
    cp /tmp/libduckdb/libduckdb.so /usr/local/lib/

RUN CGO_ENABLED=1 CGO_LDFLAGS="-L/usr/local/lib" go build -tags "duckdb_use_lib netgo" -o /duckpond /src/...

RUN /duckpond -load-extensions

FROM debian:bookworm-slim

# Install dependencies for extension download script
RUN apt-get update && apt-get install -y \
    jq \
    wget \
    && rm -rf /var/lib/apt/lists/*

# /root includes .duckdb/extensions
COPY --from=build /root /root
COPY --from=build /duckpond /duckpond
COPY --from=build /usr/local/lib/libduckdb.so /usr/local/lib/libduckdb.so

ENV LD_LIBRARY_PATH=/usr/local/lib

ENTRYPOINT [ "/duckpond", "-port", "8080", "-load-extensions"]
