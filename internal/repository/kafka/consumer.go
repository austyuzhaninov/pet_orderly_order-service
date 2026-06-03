package kafka

import (
	"context"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// MessageHandler — интерфейс обработчика Kafka сообщений.
// Реализуется в delivery/kafka/handler.go.
// Такой подход позволяет consumer не знать о usecase напрямую.
type MessageHandler interface {
	Handle(ctx context.Context, msg kafka.Message) error
}

// Consumer читает сообщения из Kafka и передаёт их в MessageHandler.
// Отвечает только за: polling, commit offset, retry, graceful shutdown.
// Не знает о бизнес-логике — это зона ответственности delivery/kafka/handler.go.
type Consumer struct {
	reader  *kafka.Reader
	handler MessageHandler
	logger  *slog.Logger
}

func NewConsumer(
	brokers []string,
	groupID string,
	topics []string,
	handler MessageHandler,
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
		reader:  reader,
		handler: handler,
		logger:  logger,
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
				c.logger.Info("kafka consumer stopped")
				return
			}
			c.logger.Error("kafka fetch message failed", "error", err)
			continue
		}

		log := c.logger.With(
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
		)

		if err := withRetry(ctx, maxRetries, retryDelay, func() error {
			return c.handler.Handle(ctx, msg)
		}); err != nil {
			log.Error("handle message failed after retries", "error", err)
			// В Фазе 3 здесь будет публикация в DLQ
		}

		// Коммитим offset после обработки (at-least-once delivery).
		// Если сервис упадёт до коммита — сообщение придёт снова,
		// поэтому handler должен быть idempotent.
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Error("kafka commit failed", "error", err)
		}
	}
}

// Close закрывает reader. Вызывается при graceful shutdown.
func (c *Consumer) Close() error {
	return c.reader.Close()
}

// withRetry выполняет fn с повторными попытками при ошибке.
// Backoff: delay * 2^attempt → 1s, 2s, 4s.
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
	return err
}

const (
	maxRetries = 3
	retryDelay = time.Second
)
