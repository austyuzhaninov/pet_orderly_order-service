package postgres

import "github.com/austyuzhaninov/pet_orderly_order-service/internal/usecase"

// Compile-time проверка что репозитории реализуют нужные интерфейсы.
// Если интерфейс не реализован — код не скомпилируется с понятной ошибкой.
var (
	_ usecase.OrderRepository  = (*OrderRepo)(nil)
	_ usecase.OutboxRepository = (*OutboxRepo)(nil)
)
