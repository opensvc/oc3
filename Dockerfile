FROM golang:latest AS builder

ARG BRANCH=${BRANCH:-main}
ARG OSVC_GITREPO_URL=${OSVC_GITREPO_URL:-https://github.com/opensvc/oc3.git}

WORKDIR /opt

RUN git clone $OSVC_GITREPO_URL && echo "Cache busted at $(date): git clone $OSVC_GITREPO_URL"

WORKDIR /opt/oc3

RUN git checkout $BRANCH && echo "Cache busted at $(date): git checkout $BRANCH"

RUN make version

RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o dist/oc3 .

RUN echo "Cache busted at $(date): oc3 version: $(./dist/oc3 version)"

FROM alpine:3.20.1

RUN apk add --no-cache bash

COPY --from=builder /opt/oc3/dist/oc3 /usr/bin/oc3

ENTRYPOINT ["/usr/bin/oc3"]
CMD ["worker"]

LABEL \
    org.opencontainers.image.authors="OpenSVC SAS" \
    org.opencontainers.image.created="${BUILDTIME}" \
    org.opencontainers.image.licenses="Apache-2.0" \
    org.opencontainers.image.url="https://github.com/opensvc/oc3"
