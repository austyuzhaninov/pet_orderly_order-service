# =============================================================================
# Multi-stage build
# Stage 1 (builder): компилируем бинарник
# Stage 2 (runtime): минимальный образ только с бинарником
# =============================================================================

# Stage 1 — builder
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Копируем go.mod и go.sum первыми — Docker кэширует этот слой
# и не перекачивает зависимости при изменении только кода
COPY go.mod go.sum ./
RUN go mod download

# Копируем весь исходный код
COPY . .

# Собираем бинарник
# CGO_ENABLED=0 — статическая линковка (нет зависимостей от libc)
# -ldflags="-w -s" — убираем отладочную информацию (меньший размер)
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-w -s" \
    -o /app/bin/service \
    ./cmd/...

# Stage 2 — runtime
FROM alpine:3.19

# Устанавливаем ca-certificates для HTTPS запросов (Jaeger OTLP)
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Копируем только бинарник и миграции из builder
COPY --from=builder /app/bin/service ./service
COPY --from=builder /app/migrations ./migrations

# Непривилегированный пользователь
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

EXPOSE 8080

ENTRYPOINT ["./service"]