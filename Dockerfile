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

RUN ["sh", "-c", "version=\"${BUILD_VERSION:-dev}\"; case \"$version\" in ''|*[!A-Za-z0-9._+-]*) echo \"VERSION must contain only letters, digits, '.', '_', '+', or '-'\" >&2; exit 1;; esac"]

COPY --link . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    ["sh", "-c", "version=\"${BUILD_VERSION:-dev}\"; CGO_ENABLED=0 GOOS=\"${TARGETOS:-linux}\" GOARCH=\"${TARGETARCH:-amd64}\" go build -trimpath -ldflags=\"-s -w -X main.version=$version -X main.toolImageRepository=${TOOL_IMAGE_REPOSITORY}\" -o /out/pvc-migrate ./cmd/pvc-migrate"]

FROM alpine:${ALPINE_VERSION}

ARG VERSION=dev
ARG TOOL_IMAGE_REPOSITORY=ghcr.io/labring-sigs/pvc-migrate

# Match upstream pv-migrate's tool images: Alpine must unlock root before
# sshd accepts the chart-mounted root public key, and UID/GID 10000 is the
# fixed identity used by its non-root tool mode.
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
COPY --link --chmod=0755 docker/sh /usr/local/bin/sh
COPY --from=build /out/pvc-migrate /usr/local/bin/pvc-migrate

LABEL org.opencontainers.image.title="pvc-migrate tool image" \
      org.opencontainers.image.description="pvc-migrate CLI and in-cluster PVC reservation, rsync, SSHD, and rclone tools" \
      org.opencontainers.image.source="https://github.com/labring-sigs/pvc-migrate" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.ref.name="${TOOL_IMAGE_REPOSITORY}:${VERSION}"

ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    HOME=/home/pvmigrate
USER 10000:10000
EXPOSE 22 2222
ENTRYPOINT ["tini", "--", "/usr/local/bin/pvc-migrate"]
