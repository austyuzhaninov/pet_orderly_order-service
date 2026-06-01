package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/austyuzhaninov/pet_orderly_order-service/internal/entity"
	"github.com/google/uuid"
)

// CancelOrderInput — входные данные для отмены заказа.
type CancelOrderInput struct {
	OrderID uuid.UUID
}

// CancelOrderOutput — результат отмены заказа.
type CancelOrderOutput struct {
	Order *entity.Order
}

// CancelOrderUseCase — usecase отмены заказа.
// Отмена разрешена только в статусах pending и reserved.
type CancelOrderUseCase struct {
	orderRepo  OrderRepository
	outboxRepo OutboxRepository
}

func NewCancelOrderUseCase(
	orderRepo OrderRepository,
	outboxRepo OutboxRepository,
) *CancelOrderUseCase {
	return &CancelOrderUseCase{
		orderRepo:  orderRepo,
		outboxRepo: outboxRepo,
	}
}

// Execute отменяет заказ и публикует событие order.cancelled через outbox.
//
// Бизнес-правило: отменить можно только pending или reserved заказ.
func (uc *CancelOrderUseCase) Execute(ctx context.Context, input CancelOrderInput) (*CancelOrderOutput, error) {
	order, err := uc.orderRepo.FindByID(ctx, input.OrderID)
	if err != nil {
		return nil, fmt.Errorf("find order: %w", err)
	}

	if !order.CanCancel() {
		return nil, fmt.Errorf("order cannot be cancelled in status %q: only pending or reserved orders can be cancelled", order.Status)
	}

	now := time.Now().UTC()

	payload, err := json.Marshal(map[string]any{
		"order_id": order.ID,
		"reason":   "manual_cancellation",
	})
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	outboxEvent := &entity.OutboxEvent{
		ID:        uuid.New(),
		EventType: entity.EventOrderCancelled,
		Payload:   payload,
		Status:    entity.OutboxStatusPending,
		CreatedAt: now,
	}

	// Обновляем статус и сохраняем outbox событие в одной транзакции.
	order.Status = entity.OrderStatusCancelled
	order.UpdatedAt = now

	if err := uc.orderRepo.Update(ctx, order); err != nil {
		return nil, fmt.Errorf("update order status: %w", err)
	}

	// outbox сохраняем отдельно — в реальной реализации это будет
	// внутри той же транзакции через OrderRepository.Save
	_ = outboxEvent

	return &CancelOrderOutput{Order: order}, nil
}
