package main

// Точка входа будет реализована в шаге 4 (delivery слой).
// Здесь будут:
//   - инициализация конфигурации (viper)
//   - подключение к PostgreSQL
//   - подключение к Kafka
//   - wire DI — сборка всех зависимостей
//   - запуск HTTP сервера (Echo)
//   - запуск Outbox Worker
//   - запуск Kafka consumer
//   - graceful shutdown по SIGTERM

func main() {}
