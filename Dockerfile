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

COPY --from=build /root /root
COPY --from=build /duckpond /duckpond

ENTRYPOINT [ "/duckpond", "-port", "8080" ]
