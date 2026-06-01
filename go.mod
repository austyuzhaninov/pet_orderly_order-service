module github.com/austyuzhaninov/pet_orderly_order-service

go 1.22

require (
	github.com/google/uuid v1.6.0
	github.com/jmoiron/sqlx v1.3.5
	github.com/labstack/echo/v4 v4.12.0
	github.com/lib/pq v1.10.9
	github.com/pressly/goose/v3 v3.20.0
	github.com/prometheus/client_golang v1.19.1
	github.com/segmentio/kafka-go v0.4.47
	github.com/spf13/viper v1.19.0
	github.com/stretchr/testify v1.9.0
	github.com/swaggo/echo-swagger v1.4.1
	github.com/swaggo/swag v1.16.3
	github.com/testcontainers/testcontainers-go v0.31.0
	go.opentelemetry.io/otel v1.27.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.27.0
	go.opentelemetry.io/otel/sdk v1.27.0
	go.opentelemetry.io/otel/trace v1.27.0
	golang.org/x/time v0.5.0
)