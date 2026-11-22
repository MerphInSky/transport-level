.PHONY: run test vet build kafka up down

run:
	go run ./cmd/transport

test:
	go test -race ./...

vet:
	go vet ./...

build:
	go build -o transport ./cmd/transport

kafka:
	docker compose up -d

up:
	docker compose --profile app up --build

down:
	docker compose --profile app down
