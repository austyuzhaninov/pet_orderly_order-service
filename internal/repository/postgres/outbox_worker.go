package postgres

import (
	"context"
	"log/slog"
	"time"

	"github.com/austyuzhaninov/pet_orderly_order-service/internal/usecase"
)

const (
	outboxPollInterval = 500 * time.Millisecond
	outboxBatchSize    = 10
)

// OutboxWorker — горутина которая читает pending записи из outbox
// и публикует их в Kafka. Реализует Outbox Pattern.
//
// Запускается один раз при старте сервиса.
// Корректно завершается при отмене контекста (graceful shutdown).
type OutboxWorker struct {
	outboxRepo usecase.OutboxRepository
	publisher  usecase.EventPublisher
	logger     *slog.Logger
}

func NewOutboxWorker(
	outboxRepo usecase.OutboxRepository,
	publisher usecase.EventPublisher,
	logger *slog.Logger,
) *OutboxWorker {
	return &OutboxWorker{
		outboxRepo: outboxRepo,
		publisher:  publisher,
		logger:     logger,
	}
}

// Run запускает polling loop.
// Блокирует горутину до отмены ctx.
func (w *OutboxWorker) Run(ctx context.Context) {
	w.logger.Info("outbox worker started")
	ticker := time.NewTicker(outboxPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("outbox worker stopped")
			return
		case <-ticker.C:
			w.processBatch(ctx)
		}
	}
}

// processBatch читает батч pending событий и публикует каждое в Kafka.
func (w *OutboxWorker) processBatch(ctx context.Context) {
	events, err := w.outboxRepo.FindPending(ctx, outboxBatchSize)
	if err != nil {
		w.logger.Error("outbox: find pending failed", "error", err)
		return
	}

	for _, event := range events {
		log := w.logger.With(
			"event_id", event.ID,
			"event_type", event.EventType,
		)

		if err := w.publisher.Publish(ctx, event.EventType, event.ID.String(), event.Payload); err != nil {
			log.Error("outbox: publish failed", "error", err)

			if markErr := w.outboxRepo.MarkFailed(ctx, event.ID); markErr != nil {
				log.Error("outbox: mark failed error", "error", markErr)
			}
			continue
		}

		if err := w.outboxRepo.MarkProcessed(ctx, event.ID); err != nil {
			// Событие опубликовано в Kafka но не помечено в БД.
			// При следующем poll Worker попробует опубликовать снова —
			// Kafka consumer должен быть idempotent (inventory-service это учитывает).
			log.Error("outbox: mark processed error", "error", err)
			continue
		}

		log.Info("outbox: event published")
	}
}
