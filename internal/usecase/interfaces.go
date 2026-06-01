package usecase

import (
	"context"

	"github.com/austyuzhaninov/pet_orderly_order-service/internal/entity"
	"github.com/google/uuid"
)

// OrderRepository — интерфейс для работы с хранилищем заказов.
// Реализуется в repository/postgres/order_repo.go.
// usecase знает только об этом интерфейсе, не о конкретной БД.
type OrderRepository interface {
	// Save сохраняет новый заказ и outbox событие в одной транзакции.
	Save(ctx context.Context, order *entity.Order, event *entity.OutboxEvent) error

	// FindByID возвращает заказ по ID.
	// Возвращает ErrOrderNotFound если заказ не найден.
	FindByID(ctx context.Context, id uuid.UUID) (*entity.Order, error)

	// Update обновляет поля заказа (quantity, updated_at).
	Update(ctx context.Context, order *entity.Order) error

	// UpdateStatus обновляет статус заказа.
	UpdateStatus(ctx context.Context, id uuid.UUID, status entity.OrderStatus) error
}

// OutboxRepository — интерфейс для работы с outbox таблицей.
// Реализуется в repository/postgres/outbox_repo.go.
type OutboxRepository interface {
	// FindPending возвращает непроведённые записи outbox (limit штук).
	FindPending(ctx context.Context, limit int) ([]*entity.OutboxEvent, error)

	// MarkProcessed помечает запись как успешно опубликованную.
	MarkProcessed(ctx context.Context, id uuid.UUID) error

	// MarkFailed помечает запись как неудачную.
	MarkFailed(ctx context.Context, id uuid.UUID) error
}

// EventPublisher — интерфейс для публикации событий в Kafka.
// Реализуется в repository/kafka/producer.go.
type EventPublisher interface {
	// Publish публикует событие в указанный топик Kafka.
	// Принимает context для отмены и трассировки.
	Publish(ctx context.Context, topic string, key string, payload []byte) error
}
