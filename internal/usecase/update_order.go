package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/austyuzhaninov/pet_orderly_order-service/internal/entity"
	"github.com/google/uuid"
)

// UpdateOrderInput — входные данные для редактирования заказа.
type UpdateOrderInput struct {
	OrderID  uuid.UUID
	Quantity int
}

// UpdateOrderOutput — результат редактирования заказа.
type UpdateOrderOutput struct {
	Order *entity.Order
}

// UpdateOrderUseCase — usecase редактирования заказа.
// Редактирование разрешено только в статусе pending.
type UpdateOrderUseCase struct {
	orderRepo OrderRepository
}

func NewUpdateOrderUseCase(orderRepo OrderRepository) *UpdateOrderUseCase {
	return &UpdateOrderUseCase{orderRepo: orderRepo}
}

// Execute обновляет quantity заказа.
//
// Бизнес-правило: редактировать можно только заказ в статусе pending.
func (uc *UpdateOrderUseCase) Execute(ctx context.Context, input UpdateOrderInput) (*UpdateOrderOutput, error) {
	if input.Quantity <= 0 {
		return nil, fmt.Errorf("quantity must be greater than 0")
	}

	order, err := uc.orderRepo.FindByID(ctx, input.OrderID)
	if err != nil {
		return nil, fmt.Errorf("find order: %w", err)
	}

	if !order.CanUpdate() {
		return nil, fmt.Errorf("order cannot be updated in status %q: only pending orders can be updated", order.Status)
	}

	order.Quantity = input.Quantity
	order.UpdatedAt = time.Now().UTC()

	if err := uc.orderRepo.Update(ctx, order); err != nil {
		return nil, fmt.Errorf("update order: %w", err)
	}

	return &UpdateOrderOutput{Order: order}, nil
}
