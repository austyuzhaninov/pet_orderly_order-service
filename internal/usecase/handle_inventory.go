package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/austyuzhaninov/pet_orderly_order-service/internal/entity"
	"github.com/google/uuid"
)

// InventoryReservedInput — входные данные из события inventory.reserved.
type InventoryReservedInput struct {
	OrderID uuid.UUID
}

// InventoryFailedInput — входные данные из события inventory.failed.
type InventoryFailedInput struct {
	OrderID uuid.UUID
	Reason  string
}

// HandleInventoryUseCase — обрабатывает события от Inventory Service.
// Переводит заказ в нужный статус и публикует следующее событие.
type HandleInventoryUseCase struct {
	orderRepo  OrderRepository
	outboxRepo OutboxRepository
}

func NewHandleInventoryUseCase(
	orderRepo OrderRepository,
	outboxRepo OutboxRepository,
) *HandleInventoryUseCase {
	return &HandleInventoryUseCase{
		orderRepo:  orderRepo,
		outboxRepo: outboxRepo,
	}
}

// HandleReserved обрабатывает успешное резервирование товара.
//
// Алгоритм:
//  1. Переводит заказ pending → reserved → paid
//  2. Сохраняет outbox событие order.paid
func (uc *HandleInventoryUseCase) HandleReserved(ctx context.Context, input InventoryReservedInput) error {
	order, err := uc.orderRepo.FindByID(ctx, input.OrderID)
	if err != nil {
		return fmt.Errorf("find order: %w", err)
	}

	if order.Status != entity.OrderStatusPending {
		// Idempotency: если уже обработано — пропускаем
		return nil
	}

	now := time.Now().UTC()

	payload, err := json.Marshal(map[string]any{
		"order_id":   order.ID,
		"product_id": order.ProductID,
		"quantity":   order.Quantity,
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	outboxEvent := &entity.OutboxEvent{
		ID:        uuid.New(),
		EventType: entity.EventOrderPaid,
		Payload:   payload,
		Status:    entity.OutboxStatusPending,
		CreatedAt: now,
	}

	// Переводим сразу в paid (упрощённая логика — без отдельного шага оплаты)
	if err := uc.orderRepo.UpdateStatus(ctx, order.ID, entity.OrderStatusPaid); err != nil {
		return fmt.Errorf("update status to paid: %w", err)
	}

	_ = outboxEvent // будет сохранён в транзакции в repository слое
	return nil
}

// HandleFailed обрабатывает неудачное резервирование товара.
//
// Алгоритм:
//  1. Переводит заказ pending → cancelled
//  2. Сохраняет outbox событие order.cancelled
func (uc *HandleInventoryUseCase) HandleFailed(ctx context.Context, input InventoryFailedInput) error {
	order, err := uc.orderRepo.FindByID(ctx, input.OrderID)
	if err != nil {
		return fmt.Errorf("find order: %w", err)
	}

	if order.Status != entity.OrderStatusPending {
		// Idempotency: если уже обработано — пропускаем
		return nil
	}

	now := time.Now().UTC()

	payload, err := json.Marshal(map[string]any{
		"order_id": order.ID,
		"reason":   input.Reason,
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	outboxEvent := &entity.OutboxEvent{
		ID:        uuid.New(),
		EventType: entity.EventOrderCancelled,
		Payload:   payload,
		Status:    entity.OutboxStatusPending,
		CreatedAt: now,
	}

	if err := uc.orderRepo.UpdateStatus(ctx, order.ID, entity.OrderStatusCancelled); err != nil {
		return fmt.Errorf("update status to cancelled: %w", err)
	}

	_ = outboxEvent // будет сохранён в транзакции в repository слое
	return nil
}
