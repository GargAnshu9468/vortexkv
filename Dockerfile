# ==============================================================================
# VortexKV Production Multi-Stage Dockerfile
# ==============================================================================
FROM golang:1.25-alpine AS builder

WORKDIR /build

# Copy Go module definitions and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy full source tree
COPY . .

ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -ldflags="-s -w" -o vortex-server ./cmd/vortex-server
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -ldflags="-s -w" -o vortex-cli ./cmd/vortex-cli

# ==============================================================================
# Final Minimal Runtime Image
# ==============================================================================
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S -g 10001 vortex \
    && adduser -S -u 10001 -G vortex -h /data vortex \
    && mkdir -p /data \
    && chown -R vortex:vortex /data

WORKDIR /data

COPY --from=builder /build/vortex-server /usr/local/bin/vortex-server
COPY --from=builder /build/vortex-cli /usr/local/bin/vortex-cli

ENV VORTEX_PORT=7379 \
    VORTEX_WEB_PORT=7380 \
    VORTEX_DATA_DIR=/data

LABEL maintainer="Anshu Garg <a.garg9050@gmail.com>" \
      org.opencontainers.image.title="vortexkv" \
      org.opencontainers.image.description="Ultra high-performance in-memory key-value data engine with native AI vector search and Web Command Deck" \
      org.opencontainers.image.url="https://garganshu9468.github.io/vortexkv/" \
      org.opencontainers.image.source="https://github.com/GargAnshu9468/vortexkv" \
      org.opencontainers.image.documentation="https://github.com/GargAnshu9468/vortexkv/wiki" \
      org.opencontainers.image.licenses="MIT"

USER vortex:vortex

# Port 7379: VortexKV RESP Wire Protocol
# Port 7380: Immersive Visual Studio Web Deck
EXPOSE 7379 7380

VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/vortex-server"]
CMD ["-bind", "0.0.0.0", "-port", "7379", "-web-bind", "0.0.0.0", "-web-port", "7380", "-aof", "/data/vortex.aof"]
