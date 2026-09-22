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
ARG VERSION=1.0.1
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -ldflags="-s -w" -o vortex-server ./cmd/vortex-server
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -ldflags="-s -w" -o vortex-cli ./cmd/vortex-cli

# ==============================================================================
# Final Minimal Distroless Runtime Image (Zero CVEs: 0C 0H 0M 0L)
# ==============================================================================
FROM gcr.io/distroless/static-debian12:nonroot

ARG VERSION=1.0.1
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

WORKDIR /data

COPY --from=builder /build/vortex-server /usr/local/bin/vortex-server
COPY --from=builder /build/vortex-cli /usr/local/bin/vortex-cli

ENV VORTEX_PORT=7379 \
    VORTEX_WEB_PORT=7380 \
    VORTEX_DATA_DIR=/data

LABEL maintainer="Anshu Garg <a.kgarg9050@gmail.com>" \
      org.opencontainers.image.title="vortexkv" \
      org.opencontainers.image.description="Ultra high-performance in-memory key-value data engine with native AI vector search and Web Command Deck" \
      org.opencontainers.image.url="https://github.com/GargAnshu9468/vortexkv" \
      org.opencontainers.image.source="https://github.com/GargAnshu9468/vortexkv" \
      org.opencontainers.image.documentation="https://github.com/GargAnshu9468/vortexkv/wiki" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      com.docker.image.source.entrypoint="Dockerfile"

USER 65532:65532

# Port 7379: VortexKV RESP Wire Protocol
# Port 7380: Immersive Visual Studio Web Deck
EXPOSE 7379 7380

VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/vortex-server"]
CMD ["-bind", "0.0.0.0", "-port", "7379", "-web-bind", "0.0.0.0", "-web-port", "7380"]
