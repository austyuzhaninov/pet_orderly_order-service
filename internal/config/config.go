package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config — конфигурация сервиса.
// Все поля читаются из переменных окружения через viper.
type Config struct {
	ServiceName string `mapstructure:"SERVICE_NAME"`
	HTTPPort    int    `mapstructure:"HTTP_PORT"`
	LogLevel    string `mapstructure:"LOG_LEVEL"`

	PostgresDSN string `mapstructure:"POSTGRES_DSN"`

	KafkaBrokers        []string
	KafkaGroupID        string
	KafkaConsumerTopics []string

	JaegerEndpoint string

	RateLimitRPS    float64
	ShutdownTimeout time.Duration
}

// Load читает конфигурацию из переменных окружения.
func Load() (*Config, error) {
	viper.AutomaticEnv()

	// Дефолтные значения
	viper.SetDefault("SERVICE_NAME", "order-service")
	viper.SetDefault("HTTP_PORT", 8080)
	viper.SetDefault("LOG_LEVEL", "info")
	viper.SetDefault("KAFKA_BROKERS", "localhost:9094")
	viper.SetDefault("KAFKA_GROUP_ID", "order-service-group")
	viper.SetDefault("KAFKA_CONSUMER_TOPICS", "inventory.reserved,inventory.failed")
	viper.SetDefault("RATE_LIMIT_RPS", 10.0)
	viper.SetDefault("SHUTDOWN_TIMEOUT", "30s")

	cfg := &Config{
		ServiceName:     viper.GetString("SERVICE_NAME"),
		HTTPPort:        viper.GetInt("HTTP_PORT"),
		LogLevel:        viper.GetString("LOG_LEVEL"),
		PostgresDSN:     viper.GetString("POSTGRES_DSN"),
		KafkaGroupID:    viper.GetString("KAFKA_GROUP_ID"),
		JaegerEndpoint:  viper.GetString("JAEGER_ENDPOINT"),
		RateLimitRPS:    viper.GetFloat64("RATE_LIMIT_RPS"),
		ShutdownTimeout: viper.GetDuration("SHUTDOWN_TIMEOUT"),

		// Слайсы читаем вручную — viper не умеет применять дефолты
		// для []string через Unmarshal когда значение из ENV
		KafkaBrokers:        splitTrim(viper.GetString("KAFKA_BROKERS")),
		KafkaConsumerTopics: splitTrim(viper.GetString("KAFKA_CONSUMER_TOPICS")),
	}

	if err := validate(cfg); err != nil {
		return nil, fmt.Errorf("config validation: %w", err)
	}

	return cfg, nil
}

func validate(cfg *Config) error {
	if cfg.PostgresDSN == "" {
		return fmt.Errorf("POSTGRES_DSN is required")
	}
	if len(cfg.KafkaBrokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS is required")
	}
	if len(cfg.KafkaConsumerTopics) == 0 {
		return fmt.Errorf("KAFKA_CONSUMER_TOPICS is required")
	}
	return nil
}

func splitTrim(s string) []string {
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			result = append(result, t)
		}
	}
	return result
}
