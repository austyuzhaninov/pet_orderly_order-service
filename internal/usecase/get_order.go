package usecase

import (
	"context"
	"fmt"

	"github.com/austyuzhaninov/pet_orderly_order-service/internal/entity"
	"github.com/google/uuid"
)

// GetOrderOutput — результат получения заказа.
type GetOrderOutput struct {
	Order *entity.Order
}

// GetOrderUseCase — usecase получения заказа по ID.
type GetOrderUseCase struct {
	orderRepo OrderRepository
}

func NewGetOrderUseCase(orderRepo OrderRepository) *GetOrderUseCase {
	return &GetOrderUseCase{orderRepo: orderRepo}
}

// Execute возвращает заказ по ID.
func (uc *GetOrderUseCase) Execute(ctx context.Context, id uuid.UUID) (*GetOrderOutput, error) {
	order, err := uc.orderRepo.FindByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("find order: %w", err)
	}

	return &GetOrderOutput{Order: order}, nil
}
