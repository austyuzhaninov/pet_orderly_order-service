package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/austyuzhaninov/pet_orderly_order-service/internal/config"
	deliveryHTTP "github.com/austyuzhaninov/pet_orderly_order-service/internal/delivery/http"
	deliveryKafka "github.com/austyuzhaninov/pet_orderly_order-service/internal/delivery/kafka"
	kafkaRepo "github.com/austyuzhaninov/pet_orderly_order-service/internal/repository/kafka"
	pgRepo "github.com/austyuzhaninov/pet_orderly_order-service/internal/repository/postgres"
	"github.com/austyuzhaninov/pet_orderly_order-service/internal/usecase"
	"github.com/labstack/echo/v4"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("service failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	// --- Config ---
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger.Info("starting service", "service", cfg.ServiceName, "port", cfg.HTTPPort)

	ctx := context.Background()

	// --- PostgreSQL ---
	db, err := pgRepo.NewDB(ctx, cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer db.Close()
	logger.Info("postgres connected")

	// --- Repositories ---
	orderRepo := pgRepo.NewOrderRepo(db)
	outboxRepo := pgRepo.NewOutboxRepo(db)

	// --- Kafka Producer ---
	producer := kafkaRepo.NewProducer(cfg.KafkaBrokers)
	defer producer.Close()

	// --- Use Cases ---
	createOrderUC := usecase.NewCreateOrderUseCase(orderRepo, outboxRepo)
	getOrderUC := usecase.NewGetOrderUseCase(orderRepo)
	updateOrderUC := usecase.NewUpdateOrderUseCase(orderRepo)
	cancelOrderUC := usecase.NewCancelOrderUseCase(orderRepo, outboxRepo)
	handleInventoryUC := usecase.NewHandleInventoryUseCase(orderRepo, outboxRepo)

	// --- Worker context (отменяется при shutdown) ---
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	// --- Outbox Worker ---
	outboxWorker := pgRepo.NewOutboxWorker(outboxRepo, producer, logger)
	go outboxWorker.Run(workerCtx)

	// --- Kafka Consumer ---
	kafkaHandler := deliveryKafka.NewHandler(handleInventoryUC, logger)
	consumer := kafkaRepo.NewConsumer(
		cfg.KafkaBrokers,
		cfg.KafkaGroupID,
		cfg.KafkaConsumerTopics,
		kafkaHandler,
		logger,
	)
	defer consumer.Close()
	go consumer.Run(workerCtx)

	// --- HTTP Server ---
	e := echo.New()
	e.HideBanner = true

	// Middleware
	e.Use(deliveryHTTP.TraceMiddleware)
	e.Use(deliveryHTTP.LoggingMiddleware(logger))
	e.Use(deliveryHTTP.RateLimitMiddleware(float64(cfg.RateLimitRPS)))

	// Routes
	httpHandler := deliveryHTTP.NewHandler(
		createOrderUC,
		getOrderUC,
		updateOrderUC,
		cancelOrderUC,
	)
	httpHandler.Register(e)

	// Graceful Shutdown
	serverErr := make(chan error, 1)
	go func() {
		addr := fmt.Sprintf(":%d", cfg.HTTPPort)
		logger.Info("http server started", "addr", addr)
		if err := e.Start(addr); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	select {
	case err := <-serverErr:
		return fmt.Errorf("http server error: %w", err)
	case sig := <-quit:
		logger.Info("shutdown signal received", "signal", sig)
	}

	// Останавливаем воркеры
	workerCancel()

	// Останавливаем HTTP с таймаутом
	shutdownCtx, shutdownCancel := context.WithTimeout(ctx, cfg.ShutdownTimeout)
	defer shutdownCancel()

	if err := e.Shutdown(shutdownCtx); err != nil {
		logger.Error("http server shutdown error", "error", err)
	}

	logger.Info("service stopped gracefully")
	return nil
}
