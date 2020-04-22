FROM golang:alpine AS builder
RUN apk update && apk add --no-cache git make bash
WORKDIR $GOPATH/src/github.com/fabito/azure-storage-purger
COPY . .
RUN make

FROM scratch
COPY --from=builder /go/src/github.com/fabito/azure-storage-purger/bin/azp /azp
ENTRYPOINT ["/azp"]
