package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/austyuzhaninov/pet_orderly_order-service/internal/usecase"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// Handler диспетчеризует входящие Kafka события на нужные usecase.
// Отделён от repository/kafka/consumer.go чтобы соблюдать принцип:
// delivery слой знает о usecase, но не о деталях Kafka.
type Handler struct {
	handleInventory *usecase.HandleInventoryUseCase
	logger          *slog.Logger
}

func NewHandler(
	handleInventory *usecase.HandleInventoryUseCase,
	logger *slog.Logger,
) *Handler {
	return &Handler{
		handleInventory: handleInventory,
		logger:          logger,
	}
}

// Handle вызывается из consumer для каждого входящего сообщения.
func (h *Handler) Handle(ctx context.Context, msg kafka.Message) error {
	log := h.logger.With(
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
	)

	switch msg.Topic {
	case "inventory.reserved":
		return h.handleInventoryReserved(ctx, msg, log)
	case "inventory.failed":
		return h.handleInventoryFailed(ctx, msg, log)
	default:
		log.Warn("unknown topic, skipping")
		return nil
	}
}

func (h *Handler) handleInventoryReserved(ctx context.Context, msg kafka.Message, log *slog.Logger) error {
	var payload struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		return fmt.Errorf("unmarshal inventory.reserved: %w", err)
	}

	orderID, err := uuid.Parse(payload.OrderID)
	if err != nil {
		return fmt.Errorf("parse order_id: %w", err)
	}

	log.Info("handling inventory.reserved", "order_id", orderID)

	if err := h.handleInventory.HandleReserved(ctx, usecase.InventoryReservedInput{
		OrderID: orderID,
	}); err != nil {
		return fmt.Errorf("handle reserved: %w", err)
	}

	return nil
}

func (h *Handler) handleInventoryFailed(ctx context.Context, msg kafka.Message, log *slog.Logger) error {
	var payload struct {
		OrderID string `json:"order_id"`
		Reason  string `json:"reason"`
	}
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		return fmt.Errorf("unmarshal inventory.failed: %w", err)
	}

	orderID, err := uuid.Parse(payload.OrderID)
	if err != nil {
		return fmt.Errorf("parse order_id: %w", err)
	}

	log.Info("handling inventory.failed", "order_id", orderID, "reason", payload.Reason)

	if err := h.handleInventory.HandleFailed(ctx, usecase.InventoryFailedInput{
		OrderID: orderID,
		Reason:  payload.Reason,
	}); err != nil {
		return fmt.Errorf("handle failed: %w", err)
	}

	return nil
}
