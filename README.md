# pet_orderly_order-service

Сервис управления заказами — часть проекта **orderly** (event-driven система обработки заказов).

Принимает HTTP запросы, сохраняет заказы в PostgreSQL и публикует события в Kafka через Outbox Pattern.

---

## Архитектура

```
cmd/
└── main.go                  # точка входа, DI, graceful shutdown

internal/
├── config/
│   └── config.go            # конфигурация из env переменных (viper)
├── entity/
│   └── order.go             # бизнес-модели: Order, OrderStatus, OutboxEvent
├── usecase/
│   ├── interfaces.go        # контракты: OrderRepository, OutboxRepository, EventPublisher
│   ├── create_order.go
│   ├── get_order.go
│   ├── update_order.go
│   ├── cancel_order.go
│   └── handle_inventory.go  # обработка событий от inventory-service
├── repository/
│   ├── postgres/
│   │   ├── db.go            # подключение к PostgreSQL
│   │   ├── order_repo.go    # CRUD заказов
│   │   ├── outbox_repo.go   # работа с outbox таблицей
│   │   └── outbox_worker.go # горутина публикации событий в Kafka
│   └── kafka/
│       ├── producer.go      # публикация событий
│       └── consumer.go      # чтение событий от inventory-service
└── delivery/
    ├── http/
    │   ├── handler.go       # HTTP handlers
    │   ├── health.go        # /health и /metrics
    │   └── middleware.go    # TraceMiddleware, LoggingMiddleware, RateLimitMiddleware
    └── kafka/
        └── handler.go       # диспетчеризация входящих Kafka событий

migrations/
├── 000001_create_orders.sql
└── 000002_create_outbox.sql
```

### Правило зависимостей
```
delivery → usecase → entity
repository реализует интерфейсы usecase (инверсия зависимостей)
```

---

## REST API

| Метод | Путь | Описание |
|---|---|---|
| `POST` | `/api/v1/orders` | Создать заказ |
| `GET` | `/api/v1/orders/:id` | Получить заказ |
| `PUT` | `/api/v1/orders/:id` | Редактировать заказ |
| `POST` | `/api/v1/orders/:id/cancel` | Отменить заказ |
| `GET` | `/health` | Health check |
| `GET` | `/metrics` | Prometheus метрики |

### Жизненный цикл заказа
```
pending ──► reserved ──► paid
   │            │
   └────────────┴──► cancelled

Редактировать: только pending
Отменить:      pending, reserved
```

---

## Kafka

| Топик | Роль | Описание |
|---|---|---|
| `order.created` | Producer | Публикуется через Outbox Worker после создания заказа |
| `order.paid` | Producer | После успешного резервирования товара |
| `order.cancelled` | Producer | После отмены заказа |
| `inventory.reserved` | Consumer | Переводит заказ в статус paid |
| `inventory.failed` | Consumer | Переводит заказ в статус cancelled |

### Outbox Pattern
Заказ и событие сохраняются в **одной транзакции**. Outbox Worker (горутина) читает
непроведённые записи каждые 500ms и публикует их в Kafka. Это гарантирует что событие
не потеряется даже если Kafka временно недоступна.

---

## Требования

- Go 1.22+
- PostgreSQL 16
- Kafka (локально через `pet_orderly_infra`)
- [goose](https://github.com/pressly/goose) для миграций

---

## Быстрый старт

### 1. Запустить инфраструктуру

```bash
cd ../pet_orderly_infra/docker-compose
docker compose up -d
```

### 2. Настроить переменные окружения

```bash
cp .env.example .env
# Отредактировать при необходимости
```

### 3. Применить миграции

```bash
export POSTGRES_DSN="postgres://orderly:orderly_secret@localhost:5432/orderly_orders?sslmode=disable"
goose -dir ./migrations postgres "$POSTGRES_DSN" up
```

### 4. Запустить сервис

```bash
go run ./cmd/...
```

---

## Переменные окружения

| Переменная | Дефолт | Описание |
|---|---|---|
| `SERVICE_NAME` | `order-service` | Имя сервиса (логи, трейсы) |
| `HTTP_PORT` | `8080` | Порт HTTP сервера |
| `LOG_LEVEL` | `info` | Уровень логирования |
| `POSTGRES_DSN` | — | DSN подключения к PostgreSQL (**обязательно**) |
| `KAFKA_BROKERS` | `localhost:9094` | Адреса Kafka брокеров (через запятую) |
| `KAFKA_GROUP_ID` | `order-service-group` | Consumer group ID |
| `KAFKA_CONSUMER_TOPICS` | `inventory.reserved,inventory.failed` | Топики для чтения |
| `JAEGER_ENDPOINT` | — | OTLP HTTP endpoint Jaeger |
| `RATE_LIMIT_RPS` | `10` | Лимит запросов в секунду на POST /orders |
| `SHUTDOWN_TIMEOUT` | `30s` | Таймаут graceful shutdown |

---

## Makefile

```bash
make build       # собрать бинарник → bin/order-service
make run         # go run ./cmd/...
make test        # unit тесты
make test-int    # integration тесты (требует Docker)
make lint        # golangci-lint
make migrate     # применить миграции (требует POSTGRES_DSN)
make swagger     # сгенерировать Swagger документацию
make tidy        # go mod tidy
```

---

## Примеры запросов

### Создать заказ
```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{"product_id": "11111111-0000-0000-0000-000000000001", "quantity": 2}'
```

### Получить заказ
```bash
curl http://localhost:8080/api/v1/orders/{id}
```

### Редактировать заказ (только pending)
```bash
curl -X PUT http://localhost:8080/api/v1/orders/{id} \
  -H "Content-Type: application/json" \
  -d '{"quantity": 5}'
```

### Отменить заказ (pending или reserved)
```bash
curl -X POST http://localhost:8080/api/v1/orders/{id}/cancel
```

### Health check
```bash
curl http://localhost:8080/health
# {"status":"ok"}
```

---

## Разработка

### Структура коммитов
```
chore: init go module, add deps
feat(entity): add Order and OrderStatus models
feat(usecase): define interfaces and business logic
feat(repository): implement postgres and kafka adapters
feat(delivery): HTTP handlers and Kafka consumer
feat(outbox): outbox worker for reliable event publishing
```

### Запуск линтера
```bash
# Установить golangci-lint
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin

make lint
```