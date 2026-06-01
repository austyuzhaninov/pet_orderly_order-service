package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer — реализация usecase.EventPublisher через Kafka.
type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string) *Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		// Автоматически создаёт топик если не существует.
		// В проде лучше создавать топики явно (что мы и делаем через kafka-init).
		AllowAutoTopicCreation: true,
	}

	return &Producer{writer: writer}
}

// Publish публикует сообщение в указанный топик.
// key используется для партиционирования — сообщения с одним key
// всегда попадают в одну партицию (гарантия порядка).
func (p *Producer) Publish(ctx context.Context, topic string, key string, payload []byte) error {
	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: payload,
		Time:  time.Now().UTC(),
		Headers: []kafka.Header{
			{Key: "X-Idempotency-Key", Value: []byte(key)},
			{Key: "X-Source-Service", Value: []byte("order-service")},
		},
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write kafka message topic=%s key=%s: %w", topic, key, err)
	}

	return nil
}

// Close закрывает writer. Вызывается при graceful shutdown.
func (p *Producer) Close() error {
	return p.writer.Close()
}
