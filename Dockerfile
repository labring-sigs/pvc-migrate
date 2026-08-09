ARG GO_VERSION=1.26.5
ARG ALPINE_VERSION=3.24.1

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-trixie AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG TOOL_IMAGE_REPOSITORY=ghcr.io/labring-sigs/pvc-migrate
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
    ["sh", "-c", "CGO_ENABLED=0 GOOS=\"${TARGETOS:-linux}\" GOARCH=\"${TARGETARCH:-amd64}\" go build -trimpath -ldflags=\"-s -w -X main.version=$(printenv BUILD_VERSION) -X main.toolImageRepository=${TOOL_IMAGE_REPOSITORY}\" -o /out/pvc-migrate ./cmd/pvc-migrate"]

FROM alpine:${ALPINE_VERSION}

ARG VERSION=dev
ARG TOOL_IMAGE_REPOSITORY=ghcr.io/labring-sigs/pvc-migrate

RUN apk add --no-cache \
      ca-certificates \
      openssh \
      openssh-server-pam \
      rclone \
      rsync \
      tini \
    && sed -i -e 's/^root:!:/root:*:/' /etc/shadow \
    && addgroup -g 10000 pvmigrate \
    && adduser -D -u 10000 -G pvmigrate -h /home/pvmigrate pvmigrate \
    && mkdir -p /home/pvmigrate/.ssh \
    && chown -R pvmigrate:pvmigrate /home/pvmigrate \
    && chmod 700 /home/pvmigrate/.ssh

COPY docker/sshd_config /etc/ssh/sshd_config
COPY --from=build /out/pvc-migrate /usr/local/bin/pvc-migrate

LABEL org.opencontainers.image.title="pvc-migrate all-in-one tool image" \
      org.opencontainers.image.description="Unified pvc-migrate CLI, rsync, SSHD, rclone, and PVC reservation helper" \
      org.opencontainers.image.source="https://github.com/labring-sigs/pvc-migrate" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.ref.name="${TOOL_IMAGE_REPOSITORY}:${VERSION}"

ENV HOME=/home/pvmigrate
USER 10000:10000
EXPOSE 22 2222
ENTRYPOINT ["tini", "--", "/usr/local/bin/pvc-migrate"]
