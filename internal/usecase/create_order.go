package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/austyuzhaninov/pet_orderly_order-service/internal/entity"
	"github.com/google/uuid"
)

// CreateOrderInput — входные данные для создания заказа.
type CreateOrderInput struct {
	ProductID uuid.UUID `json:"product_id"`
	Quantity  int       `json:"quantity"`
}

// CreateOrderOutput — результат создания заказа.
type CreateOrderOutput struct {
	Order *entity.Order
}

// CreateOrderUseCase — usecase создания заказа.
// Сохраняет заказ и outbox событие в одной транзакции.
type CreateOrderUseCase struct {
	orderRepo  OrderRepository
	outboxRepo OutboxRepository
}

func NewCreateOrderUseCase(
	orderRepo OrderRepository,
	outboxRepo OutboxRepository,
) *CreateOrderUseCase {
	return &CreateOrderUseCase{
		orderRepo:  orderRepo,
		outboxRepo: outboxRepo,
	}
}

// Execute создаёт новый заказ.
//
// Алгоритм:
//  1. Валидация входных данных
//  2. Создание entity.Order со статусом pending
//  3. Сериализация payload для outbox события
//  4. Сохранение заказа + outbox в одной транзакции (Outbox Pattern)
func (uc *CreateOrderUseCase) Execute(ctx context.Context, input CreateOrderInput) (*CreateOrderOutput, error) {
	if err := validateCreateInput(input); err != nil {
		return nil, fmt.Errorf("validation: %w", err)
	}

	now := time.Now().UTC()
	order := &entity.Order{
		ID:        uuid.New(),
		ProductID: input.ProductID,
		Quantity:  input.Quantity,
		Status:    entity.OrderStatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}

	payload, err := json.Marshal(map[string]any{
		"order_id":   order.ID,
		"product_id": order.ProductID,
		"quantity":   order.Quantity,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	outboxEvent := &entity.OutboxEvent{
		ID:        uuid.New(),
		EventType: entity.EventOrderCreated,
		Payload:   payload,
		Status:    entity.OutboxStatusPending,
		CreatedAt: now,
	}

	// Сохраняем заказ и outbox событие в одной транзакции.
	// Если Kafka упадёт после этого — Outbox Worker опубликует событие позже.
	if err := uc.orderRepo.Save(ctx, order, outboxEvent); err != nil {
		return nil, fmt.Errorf("save order: %w", err)
	}

	return &CreateOrderOutput{Order: order}, nil
}

func validateCreateInput(input CreateOrderInput) error {
	if input.ProductID == uuid.Nil {
		return fmt.Errorf("product_id is required")
	}
	if input.Quantity <= 0 {
		return fmt.Errorf("quantity must be greater than 0")
	}
	return nil
}
