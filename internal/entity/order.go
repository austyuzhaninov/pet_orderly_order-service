package entity

import (
	"time"

	"github.com/google/uuid"
)

// OrderStatus — статус заказа.
// Жизненный цикл:
//
//	pending → reserved → paid
//	pending → cancelled
//	reserved → cancelled
type OrderStatus string

const (
	OrderStatusPending   OrderStatus = "pending"
	OrderStatusReserved  OrderStatus = "reserved"
	OrderStatusPaid      OrderStatus = "paid"
	OrderStatusCancelled OrderStatus = "cancelled"
)

// Order — бизнес-модель заказа.
// Не зависит ни от одного внешнего пакета проекта.
type Order struct {
	ID        uuid.UUID   `db:"id"`
	ProductID uuid.UUID   `db:"product_id"`
	Quantity  int         `db:"quantity"`
	Status    OrderStatus `db:"status"`
	CreatedAt time.Time   `db:"created_at"`
	UpdatedAt time.Time   `db:"updated_at"`
}

// CanUpdate возвращает true если заказ можно редактировать.
// Редактирование разрешено только в статусе pending.
func (o *Order) CanUpdate() bool {
	return o.Status == OrderStatusPending
}

// CanCancel возвращает true если заказ можно отменить.
// Отмена разрешена в статусах pending и reserved.
func (o *Order) CanCancel() bool {
	return o.Status == OrderStatusPending || o.Status == OrderStatusReserved
}

// OutboxStatus — статус записи в outbox таблице.
type OutboxStatus string

const (
	OutboxStatusPending   OutboxStatus = "pending"
	OutboxStatusProcessed OutboxStatus = "processed"
	OutboxStatusFailed    OutboxStatus = "failed"
)

// OutboxEvent — запись в таблице outbox.
// Используется для надёжной публикации событий в Kafka (Outbox Pattern).
// Сохраняется в одной транзакции с заказом.
type OutboxEvent struct {
	ID          uuid.UUID    `db:"id"`
	EventType   string       `db:"event_type"`
	Payload     []byte       `db:"payload"`
	Status      OutboxStatus `db:"status"`
	CreatedAt   time.Time    `db:"created_at"`
	ProcessedAt *time.Time   `db:"processed_at"`
}

// Kafka event types — константы имён событий.
// Используются в OutboxEvent.EventType и при публикации в Kafka.
const (
	EventOrderCreated   = "order.created"
	EventOrderPaid      = "order.paid"
	EventOrderCancelled = "order.cancelled"
)
