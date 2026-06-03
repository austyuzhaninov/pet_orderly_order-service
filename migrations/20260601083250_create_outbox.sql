-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS outbox (
                                      id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type   VARCHAR(100) NOT NULL,
    payload      JSONB        NOT NULL,
    status       VARCHAR(20)  NOT NULL DEFAULT 'pending',
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ,

    CONSTRAINT outbox_status_check
    CHECK (status IN ('pending', 'processed', 'failed'))
    );

CREATE INDEX IF NOT EXISTS idx_outbox_status_created
    ON outbox (status, created_at)
    WHERE status = 'pending';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS outbox;
-- +goose StatementEnd