BINARY := slurm_exporter 

.PHONY: all fmt vet lint build clean

all: fmt vet lint build

fmt:
	go fmt ./...

vet:
	go vet ./...

lint:
	golangci-lint run

build:
	mkdir -p bin
	go build \
		-ldflags="-X main.version=$(VERSION)" \
		-o bin/$(BINARY) \
		./cmd/slurm_exporter

clean:
	rm -rf bin/
