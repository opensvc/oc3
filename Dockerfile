FROM golang:latest AS builder

ARG BUILDTIME
ARG VERSION

WORKDIR /opt/oc3
COPY . .

RUN echo "VERSION=${VERSION}"

RUN echo "BUILDTIME=${BUILDTIME}"

RUN echo "${VERSION}" > util/version/text/VERSION

RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o dist/oc3 .

RUN dist/oc3 version

RUN echo "Cache busted at $(date): oc3 version: $(./dist/oc3 version)"

FROM alpine:3.22.4
ARG BUILDTIME

RUN apk add --no-cache bash

COPY --from=builder /opt/oc3/dist/oc3 /usr/bin/oc3

ENTRYPOINT ["/usr/bin/oc3"]
CMD ["worker"]

LABEL \
    org.opencontainers.image.authors="OpenSVC SAS" \
    org.opencontainers.image.created="${BUILDTIME}" \
    org.opencontainers.image.licenses="Apache-2.0" \
    org.opencontainers.image.url="https://github.com/opensvc/oc3"
