package http

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"golang.org/x/time/rate"
)

// TraceMiddleware добавляет trace_id в каждый запрос и прокидывает его в контекст.
// trace_id используется в логах и трассировке.
func TraceMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		traceID := c.Request().Header.Get("X-Trace-ID")
		if traceID == "" {
			traceID = uuid.New().String()
		}

		// Прокидываем trace_id в заголовок ответа
		c.Response().Header().Set("X-Trace-ID", traceID)

		// Кладём в контекст echo для использования в handlers
		c.Set("trace_id", traceID)

		return next(c)
	}
}

// LoggingMiddleware логирует каждый HTTP запрос со статусом и временем обработки.
func LoggingMiddleware(logger *slog.Logger) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			start := time.Now()

			err := next(c)

			traceID, _ := c.Get("trace_id").(string)

			logger.Info("http request",
				"trace_id", traceID,
				"method", c.Request().Method,
				"path", c.Request().URL.Path,
				"status", c.Response().Status,
				"duration_ms", time.Since(start).Milliseconds(),
				"ip", c.RealIP(),
			)

			return err
		}
	}
}

// RateLimitMiddleware ограничивает количество запросов на IP.
// Алгоритм: token bucket через golang.org/x/time/rate.
// Лимит: rps запросов в секунду на IP.
func RateLimitMiddleware(rps float64) echo.MiddlewareFunc {
	// limiters хранит лимитер для каждого IP.
	// В продакшене лучше использовать sync.Map или Redis для distributed rate limiting.
	limiters := make(map[string]*rate.Limiter)

	getLimiter := func(ip string) *rate.Limiter {
		if l, ok := limiters[ip]; ok {
			return l
		}
		l := rate.NewLimiter(rate.Limit(rps), int(rps))
		limiters[ip] = l
		return l
	}

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			ip := c.RealIP()
			limiter := getLimiter(ip)

			if !limiter.Allow() {
				return c.JSON(http.StatusTooManyRequests, map[string]string{
					"error": "rate limit exceeded",
				})
			}

			return next(c)
		}
	}
}
