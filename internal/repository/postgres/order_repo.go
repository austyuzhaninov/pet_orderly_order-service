package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/austyuzhaninov/pet_orderly_order-service/internal/entity"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// ErrOrderNotFound возвращается когда заказ не найден в БД.
var ErrOrderNotFound = errors.New("order not found")

// OrderRepo — реализация usecase.OrderRepository через PostgreSQL.
type OrderRepo struct {
	db *sqlx.DB
}

func NewOrderRepo(db *sqlx.DB) *OrderRepo {
	return &OrderRepo{db: db}
}

// Save сохраняет новый заказ и outbox событие в одной транзакции.
// Это ключевой момент Outbox Pattern — атомарность гарантирует что
// либо оба записаны, либо ни один.
func (r *OrderRepo) Save(ctx context.Context, order *entity.Order, event *entity.OutboxEvent) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	const orderQuery = `
		INSERT INTO orders (id, product_id, quantity, status, created_at, updated_at)
		VALUES (:id, :product_id, :quantity, :status, :created_at, :updated_at)
	`
	if _, err = tx.NamedExecContext(ctx, orderQuery, order); err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	const outboxQuery = `
		INSERT INTO outbox (id, event_type, payload, status, created_at)
		VALUES (:id, :event_type, :payload, :status, :created_at)
	`
	if _, err = tx.NamedExecContext(ctx, outboxQuery, event); err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}

// FindByID возвращает заказ по ID.
// Возвращает ErrOrderNotFound если заказ не найден.
func (r *OrderRepo) FindByID(ctx context.Context, id uuid.UUID) (*entity.Order, error) {
	const query = `
		SELECT id, product_id, quantity, status, created_at, updated_at
		FROM orders
		WHERE id = $1
	`

	var order entity.Order
	if err := r.db.GetContext(ctx, &order, query, id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrOrderNotFound
		}
		return nil, fmt.Errorf("get order: %w", err)
	}

	return &order, nil
}

// Update обновляет quantity и updated_at заказа.
func (r *OrderRepo) Update(ctx context.Context, order *entity.Order) error {
	const query = `
		UPDATE orders
		SET quantity = :quantity, updated_at = :updated_at
		WHERE id = :id
	`

	order.UpdatedAt = time.Now().UTC()

	result, err := r.db.NamedExecContext(ctx, query, order)
	if err != nil {
		return fmt.Errorf("update order: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if rows == 0 {
		return ErrOrderNotFound
	}

	return nil
}

// UpdateStatus обновляет только статус заказа.
func (r *OrderRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status entity.OrderStatus) error {
	const query = `
		UPDATE orders
		SET status = $1, updated_at = $2
		WHERE id = $3
	`

	result, err := r.db.ExecContext(ctx, query, status, time.Now().UTC(), id)
	if err != nil {
		return fmt.Errorf("update order status: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if rows == 0 {
		return ErrOrderNotFound
	}

	return nil
}
