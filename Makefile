# s3lister build
#
#   make          build ./s3lister (version-stamped from git)
#   make bench    build ./s3lister-bench (benchmark bucket populator)
#   make test     run the test suite
#   make clean    remove built binaries

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS  = -s -w -X main.version=$(VERSION)

.PHONY: all build bench test clean

all: build

build:
	CGO_ENABLED=0 go build -ldflags '$(LDFLAGS)' -o s3lister .

bench:
	CGO_ENABLED=0 go build -ldflags '-s -w' -o s3lister-bench ./cmd/s3lister-bench

test:
	go test ./...

clean:
	rm -f s3lister s3lister-bench
