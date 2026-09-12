.PHONY: all build test clean run benchmark install docker-build docker-run

all: build

build:
	@mkdir -p bin
	go build -o bin/vortex-server ./cmd/vortex-server
	go build -o bin/vortex-cli ./cmd/vortex-cli
	@echo "✓ Binaries built successfully in bin/"

install: build
	@echo "Installing binaries to /usr/local/bin..."
	install -m 755 bin/vortex-server /usr/local/bin/vortex-server
	install -m 755 bin/vortex-cli /usr/local/bin/vortex-cli
	@echo "✓ VortexKV installed to /usr/local/bin"

test:
	go test -v ./...

run: build
	./bin/vortex-server -requirepass "vortex_secure_2026" -maxmemory 1gb

benchmark:
	redis-benchmark -p 7379 -a "vortex_secure_2026" -t set,get -n 50000 -q -c 50

docker-build:
	docker build -t vortexkv:latest .

docker-run:
	docker run -d --name vortexkv -p 7379:7379 -p 7380:7380 -v vortex_data:/data vortexkv:latest

clean:
	rm -rf bin/ vortex.aof
