package http

import (
	"errors"
	"net/http"

	"github.com/austyuzhaninov/pet_orderly_order-service/internal/repository/postgres"
	"github.com/austyuzhaninov/pet_orderly_order-service/internal/usecase"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

// Handler содержит все HTTP handlers сервиса.
// Зависит только от usecase — не знает о repository.
type Handler struct {
	createOrder *usecase.CreateOrderUseCase
	getOrder    *usecase.GetOrderUseCase
	updateOrder *usecase.UpdateOrderUseCase
	cancelOrder *usecase.CancelOrderUseCase
}

func NewHandler(createOrder *usecase.CreateOrderUseCase, getOrder *usecase.GetOrderUseCase, updateOrder *usecase.UpdateOrderUseCase, cancelOrder *usecase.CancelOrderUseCase) *Handler {
	return &Handler{
		createOrder: createOrder,
		getOrder:    getOrder,
		updateOrder: updateOrder,
		cancelOrder: cancelOrder,
	}
}

// Register регистрирует все маршруты на Echo инстансе.
func (h *Handler) Register(e *echo.Echo) {
	v1 := e.Group("/api/v1")
	v1.POST("/orders", h.CreateOrder)
	v1.GET("/orders/:id", h.GetOrder)
	v1.PUT("/orders/:id", h.UpdateOrder)
	v1.POST("/orders/:id/cancel", h.CancelOrder)
}

// createOrderRequest — тело запроса для POST /orders.
type createOrderRequest struct {
	ProductID string `json:"product_id" validate:"required"`
	Quantity  int    `json:"quantity"   validate:"required,gt=0"`
}

// CreateOrder godoc
// @Summary     Создать заказ
// @Tags        orders
// @Accept      json
// @Produce     json
// @Param       request body createOrderRequest true "Данные заказа"
// @Success     201 {object} map[string]any
// @Failure     400 {object} map[string]string
// @Failure     429 {object} map[string]string
// @Router      /api/v1/orders [post]
func (h *Handler) CreateOrder(c echo.Context) error {
	var req createOrderRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("invalid request body"))
	}

	productID, err := uuid.Parse(req.ProductID)
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("invalid product_id format"))
	}

	out, err := h.createOrder.Execute(c.Request().Context(), usecase.CreateOrderInput{
		ProductID: productID,
		Quantity:  req.Quantity,
	})
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse(err.Error()))
	}

	return c.JSON(http.StatusCreated, map[string]any{
		"id":         out.Order.ID,
		"product_id": out.Order.ProductID,
		"quantity":   out.Order.Quantity,
		"status":     out.Order.Status,
		"created_at": out.Order.CreatedAt,
	})
}

// GetOrder godoc
// @Summary     Получить заказ
// @Tags        orders
// @Produce     json
// @Param       id path string true "Order ID"
// @Success     200 {object} map[string]any
// @Failure     400 {object} map[string]string
// @Failure     404 {object} map[string]string
// @Router      /api/v1/orders/{id} [get]
func (h *Handler) GetOrder(c echo.Context) error {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("invalid order id"))
	}

	// GetOrder пока реализован напрямую — добавим отдельный usecase если понадобится
	_ = id
	return c.JSON(http.StatusNotImplemented, errorResponse("not implemented yet"))
}

// updateOrderRequest — тело запроса для PUT /orders/:id.
type updateOrderRequest struct {
	Quantity int `json:"quantity" validate:"required,gt=0"`
}

// UpdateOrder godoc
// @Summary     Обновить заказ
// @Tags        orders
// @Accept      json
// @Produce     json
// @Param       id      path string             true "Order ID"
// @Param       request body updateOrderRequest true "Новые данные"
// @Success     200 {object} map[string]any
// @Failure     400 {object} map[string]string
// @Failure     404 {object} map[string]string
// @Router      /api/v1/orders/{id} [put]
func (h *Handler) UpdateOrder(c echo.Context) error {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("invalid order id"))
	}

	var req updateOrderRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("invalid request body"))
	}

	out, err := h.updateOrder.Execute(c.Request().Context(), usecase.UpdateOrderInput{
		OrderID:  id,
		Quantity: req.Quantity,
	})
	if err != nil {
		if errors.Is(err, postgres.ErrOrderNotFound) {
			return c.JSON(http.StatusNotFound, errorResponse("order not found"))
		}
		return c.JSON(http.StatusBadRequest, errorResponse(err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{
		"id":         out.Order.ID,
		"product_id": out.Order.ProductID,
		"quantity":   out.Order.Quantity,
		"status":     out.Order.Status,
		"updated_at": out.Order.UpdatedAt,
	})
}

// CancelOrder godoc
// @Summary     Отменить заказ
// @Tags        orders
// @Produce     json
// @Param       id path string true "Order ID"
// @Success     200 {object} map[string]any
// @Failure     400 {object} map[string]string
// @Failure     404 {object} map[string]string
// @Router      /api/v1/orders/{id}/cancel [post]
func (h *Handler) CancelOrder(c echo.Context) error {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("invalid order id"))
	}

	out, err := h.cancelOrder.Execute(c.Request().Context(), usecase.CancelOrderInput{
		OrderID: id,
	})
	if err != nil {
		if errors.Is(err, postgres.ErrOrderNotFound) {
			return c.JSON(http.StatusNotFound, errorResponse("order not found"))
		}
		return c.JSON(http.StatusBadRequest, errorResponse(err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{
		"id":         out.Order.ID,
		"status":     out.Order.Status,
		"updated_at": out.Order.UpdatedAt,
	})
}

func errorResponse(msg string) map[string]string {
	return map[string]string{"error": msg}
}
