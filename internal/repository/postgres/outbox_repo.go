package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/austyuzhaninov/pet_orderly_order-service/internal/entity"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// OutboxRepo — реализация usecase.OutboxRepository через PostgreSQL.
// Используется Outbox Worker для чтения и обновления статусов событий.
type OutboxRepo struct {
	db *sqlx.DB
}

func NewOutboxRepo(db *sqlx.DB) *OutboxRepo {
	return &OutboxRepo{db: db}
}

// FindPending возвращает непроведённые outbox события.
// Limit ограничивает размер батча — Worker обрабатывает по N записей за раз.
func (r *OutboxRepo) FindPending(ctx context.Context, limit int) ([]*entity.OutboxEvent, error) {
	const query = `
		SELECT id, event_type, payload, status, created_at, processed_at
		FROM outbox
		WHERE status = 'pending'
		ORDER BY created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`
	// FOR UPDATE SKIP LOCKED — важная деталь:
	// при нескольких репликах сервиса каждая берёт свой батч
	// без конкуренции и без блокировок друг друга.

	var events []*entity.OutboxEvent
	if err := r.db.SelectContext(ctx, &events, query, limit); err != nil {
		return nil, fmt.Errorf("select pending outbox: %w", err)
	}

	return events, nil
}

// MarkProcessed помечает outbox запись как успешно опубликованную в Kafka.
func (r *OutboxRepo) MarkProcessed(ctx context.Context, id uuid.UUID) error {
	const query = `
		UPDATE outbox
		SET status = 'processed', processed_at = $1
		WHERE id = $2
	`

	if _, err := r.db.ExecContext(ctx, query, time.Now().UTC(), id); err != nil {
		return fmt.Errorf("mark outbox processed: %w", err)
	}

	return nil
}

// MarkFailed помечает outbox запись как неудачную.
// Вызывается если публикация в Kafka завершилась ошибкой после всех retry.
func (r *OutboxRepo) MarkFailed(ctx context.Context, id uuid.UUID) error {
	const query = `
		UPDATE outbox
		SET status = 'failed', processed_at = $1
		WHERE id = $2
	`

	if _, err := r.db.ExecContext(ctx, query, time.Now().UTC(), id); err != nil {
		return fmt.Errorf("mark outbox failed: %w", err)
	}

	return nil
}
