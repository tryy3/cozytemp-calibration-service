# Build stage
FROM --platform=$BUILDPLATFORM golang:1.25.1-alpine AS builder

# Build arguments for cross-compilation
ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
# CGO_ENABLED=0 for static binary, GOOS/GOARCH for cross-compilation
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-w -s" -o /app/cozytemp-calibration-service .

# Runtime stage - using scratch for minimal image size
FROM scratch

# Copy the binary
COPY --from=builder /app/cozytemp-calibration-service /cozytemp-calibration-service

# Run as non-root (using numeric UID for scratch image)
USER 65534:65534

ENTRYPOINT ["/cozytemp-calibration-service"]

