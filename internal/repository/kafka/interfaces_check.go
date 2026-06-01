package kafka

import "github.com/austyuzhaninov/pet_orderly_order-service/internal/usecase"

// Compile-time проверка что Producer реализует интерфейс EventPublisher.
var _ usecase.EventPublisher = (*Producer)(nil)
