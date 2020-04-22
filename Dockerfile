FROM golang:alpine AS builder
RUN apk update && apk add --no-cache git make bash ca-certificates
WORKDIR $GOPATH/src/github.com/fabito/azure-storage-purger
COPY . .
ENV CGO_ENABLED=0 
RUN make

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /go/src/github.com/fabito/azure-storage-purger/bin/azp /
ENTRYPOINT ["/azp"]
