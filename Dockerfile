FROM golang:1.26-bookworm AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /vector-k8s-helper ./cmd/vector-k8s-helper

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /vector-k8s-helper /vector-k8s-helper
ENTRYPOINT ["/vector-k8s-helper"]