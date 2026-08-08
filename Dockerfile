ARG GO_VERSION=1.26.5

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-trixie AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ENV BUILD_VERSION=${VERSION}

WORKDIR /src

COPY --link go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

RUN ["sh", "-c", "case \"$(printenv BUILD_VERSION)\" in ''|*[!A-Za-z0-9._+-]*) echo \"VERSION must contain only letters, digits, '.', '_', '+', or '-'\" >&2; exit 1;; esac"]

COPY --link . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    ["sh", "-c", "CGO_ENABLED=0 GOOS=\"${TARGETOS:-linux}\" GOARCH=\"${TARGETARCH:-amd64}\" go build -trimpath -ldflags=\"-s -w -X main.version=$(printenv BUILD_VERSION)\" -o /out/pvc-migrate ./cmd/pvc-migrate"]

FROM gcr.io/distroless/static-debian13:nonroot

ARG VERSION=dev
LABEL org.opencontainers.image.title="pvc-migrate" \
      org.opencontainers.image.description="Resumable Kubernetes PVC migration and S3 backup CLI" \
      org.opencontainers.image.source="https://github.com/labring-sigs/pvc-migrate" \
      org.opencontainers.image.version="${VERSION}"

COPY --from=build /out/pvc-migrate /usr/local/bin/pvc-migrate

USER nonroot:nonroot
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/pvc-migrate"]
