package http

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RegisterSystemRoutes регистрирует служебные маршруты:
// health check и prometheus метрики.
func RegisterSystemRoutes(e *echo.Echo) {
	e.GET("/health", healthHandler)
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
}

func healthHandler(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{
		"status": "ok",
	})
}
