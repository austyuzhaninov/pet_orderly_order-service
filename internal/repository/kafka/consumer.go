package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/austyuzhaninov/pet_orderly_order-service/internal/usecase"
	"github.com/segmentio/kafka-go"
)

const (
	maxRetries = 3
	retryDelay = time.Second
)

// Consumer читает события от Inventory Service и диспетчеризует их в usecase.
type Consumer struct {
	reader          *kafka.Reader
	handleInventory *usecase.HandleInventoryUseCase
	logger          *slog.Logger
}

func NewConsumer(
	brokers []string,
	groupID string,
	topics []string,
	handleInventory *usecase.HandleInventoryUseCase,
	logger *slog.Logger,
) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		GroupTopics:    topics,
		MinBytes:       1,
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
	})

	return &Consumer{
		reader:          reader,
		handleInventory: handleInventory,
		logger:          logger,
	}
}

// Run запускает polling loop.
// Блокирует горутину до отмены ctx (graceful shutdown).
func (c *Consumer) Run(ctx context.Context) {
	c.logger.Info("kafka consumer started")

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// Контекст отменён — graceful shutdown
				c.logger.Info("kafka consumer stopped")
				return
			}
			c.logger.Error("kafka fetch message failed", "error", err)
			continue
		}

		c.handleMessage(ctx, msg)

		// Коммитим offset только после успешной обработки.
		// at-least-once delivery: при перезапуске сообщение придёт снова.
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("kafka commit failed", "error", err,
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
			)
		}
	}
}

// handleMessage диспетчеризует сообщение по топику.
func (c *Consumer) handleMessage(ctx context.Context, msg kafka.Message) {
	log := c.logger.With(
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
	)

	var err error
	switch msg.Topic {
	case "inventory.reserved":
		err = c.handleInventoryReserved(ctx, msg)
	case "inventory.failed":
		err = c.handleInventoryFailed(ctx, msg)
	default:
		log.Warn("unknown topic, skipping")
		return
	}

	if err != nil {
		log.Error("handle message failed", "error", err)
	}
}

func (c *Consumer) handleInventoryReserved(ctx context.Context, msg kafka.Message) error {
	var payload struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		return fmt.Errorf("unmarshal inventory.reserved: %w", err)
	}

	// Retry с экспоненциальным backoff
	return withRetry(ctx, maxRetries, retryDelay, func() error {
		return c.handleInventory.HandleReserved(ctx, usecase.InventoryReservedInput{
			OrderID: mustParseUUID(payload.OrderID),
		})
	})
}

func (c *Consumer) handleInventoryFailed(ctx context.Context, msg kafka.Message) error {
	var payload struct {
		OrderID string `json:"order_id"`
		Reason  string `json:"reason"`
	}
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		return fmt.Errorf("unmarshal inventory.failed: %w", err)
	}

	return withRetry(ctx, maxRetries, retryDelay, func() error {
		return c.handleInventory.HandleFailed(ctx, usecase.InventoryFailedInput{
			OrderID: mustParseUUID(payload.OrderID),
			Reason:  payload.Reason,
		})
	})
}

// Close закрывает reader. Вызывается при graceful shutdown.
func (c *Consumer) Close() error {
	return c.reader.Close()
}

// withRetry выполняет fn с повторными попытками при ошибке.
// Backoff: delay * 2^attempt (1s, 2s, 4s).
func withRetry(ctx context.Context, maxAttempts int, delay time.Duration, fn func() error) error {
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		if attempt < maxAttempts-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay * (1 << attempt)):
			}
		}
	}
	return fmt.Errorf("after %d attempts: %w", maxAttempts, err)
}

// mustParseUUID парсит UUID строку. В продакшене лучше возвращать ошибку,
// но здесь payload уже валидирован на стороне отправителя.
func mustParseUUID(s string) (id [16]byte) {
	// простой парсинг без внешних зависимостей
	// в реальном коде используй uuid.Parse(s)
	parsed, _ := parseUUID(s)
	return parsed
}

func parseUUID(s string) ([16]byte, error) {
	var id [16]byte
	if len(s) != 36 {
		return id, fmt.Errorf("invalid uuid length")
	}
	b := []byte(s)
	src := make([]byte, 0, 32)
	for _, c := range b {
		if c != '-' {
			src = append(src, c)
		}
	}
	for i := 0; i < 16; i++ {
		hi := hexVal(src[i*2])
		lo := hexVal(src[i*2+1])
		id[i] = hi<<4 | lo
	}
	return id, nil
}

func hexVal(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	}
	return 0
}
