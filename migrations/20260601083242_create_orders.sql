-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS orders (
                                      id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id  UUID        NOT NULL,
    quantity    INT         NOT NULL CHECK (quantity > 0),
    status      VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT orders_status_check
    CHECK (status IN ('pending', 'reserved', 'paid', 'cancelled'))
    );

CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders (created_at DESC);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS orders;
-- +goose StatementEnd