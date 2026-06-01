BINARY_NAME=order-service
CMD_PATH=./cmd/...
IMAGE_NAME=ghcr.io/yourusername/pet_orderly_order-service
TAG?=latest

.PHONY: lint test test-int build docker migrate swagger help

## lint: запустить golangci-lint
lint:
	golangci-lint run ./...

## test: запустить unit тесты
test:
	go test ./...

## test-int: запустить integration тесты (требует Docker)
test-int:
	go test -tags=integration -count=1 -timeout=120s ./...

## build: собрать бинарник
build:
	go build -o bin/$(BINARY_NAME) $(CMD_PATH)

## docker: собрать docker образ
docker:
	docker build -t $(IMAGE_NAME):$(TAG) .

## migrate: применить миграции БД
migrate:
	goose -dir ./migrations postgres "$(POSTGRES_DSN)" up

## migrate-down: откатить последнюю миграцию
migrate-down:
	goose -dir ./migrations postgres "$(POSTGRES_DSN)" down

## swagger: сгенерировать swagger документацию
swagger:
	swag init -g cmd/main.go -o docs/

## tidy: привести в порядок go.mod и go.sum
tidy:
	go mod tidy

## help: показать список команд
help:
	@grep -E '^## ' Makefile | sed 's/## //'
