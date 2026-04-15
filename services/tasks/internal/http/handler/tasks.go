package handler

import (
	"encoding/json"
	"kate/services/tasks/internal/amqp"
	"kate/services/tasks/internal/client"
	"kate/services/tasks/internal/service"
	"kate/shared/middleware"
	"net/http"
	"strings"

	amqplib "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type TaskHandler struct {
	taskSvc   *service.TaskService
	authGrpc  *client.AuthGrpcClient
	logger    *zap.Logger
	rabbitCh  *amqplib.Channel
	queueName string
}

// NewTaskHandler
func NewTaskHandler(ts *service.TaskService, ag *client.AuthGrpcClient, logger *zap.Logger, rabbitCh *amqplib.Channel, queueName string) *TaskHandler {
	return &TaskHandler{
		taskSvc:   ts,
		authGrpc:  ag,
		logger:    logger,
		rabbitCh:  rabbitCh,
		queueName: queueName,
	}
}

func extractToken(r *http.Request) string {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return ""
	}
	parts := strings.Split(authHeader, " ")
	if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
		return ""
	}
	return parts[1]
}

func (h *TaskHandler) verifyToken(r *http.Request) (bool, string, int) {
	reqID := middleware.GetRequestID(r.Context())
	token := extractToken(r)
	if token == "" {
		h.logger.Info("missing token", zap.String("request_id", reqID))
		return false, "", http.StatusUnauthorized
	}

	valid, subject, err := h.authGrpc.VerifyToken(r.Context(), token)
	if err != nil {
		h.logger.Error("auth verify error",
			zap.String("request_id", reqID),
			zap.Error(err),
			zap.String("component", "auth_client"),
		)
	}

	if valid && subject == "" {
		subject = "unknown"
		h.logger.Warn("subject was empty, set to unknown",
			zap.String("request_id", reqID))
	}

	if !valid {
		h.logger.Info("invalid token", zap.String("request_id", reqID))
		return false, "", http.StatusUnauthorized
	}

	h.logger.Info("token verified",
		zap.String("request_id", reqID),
		zap.String("subject", subject))
	return true, subject, http.StatusOK
}

func (h *TaskHandler) handleError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// CreateTask
func (h *TaskHandler) CreateTask(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.GetRequestID(r.Context())
	if r.Method != http.MethodPost {
		h.logger.Warn("method not allowed", zap.String("request_id", reqID), zap.String("method", r.Method))
		h.handleError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	valid, _, statusCode := h.verifyToken(r)
	if !valid {
		h.handleError(w, statusCode, "unauthorized")
		return
	}

	var task service.Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		h.logger.Warn("invalid create task body", zap.String("request_id", reqID), zap.Error(err))
		h.handleError(w, http.StatusBadRequest, "invalid json")
		return
	}

	created := h.taskSvc.Create(task)
	h.logger.Info("task created", zap.String("request_id", reqID), zap.String("task_id", created.ID))

	if h.rabbitCh != nil {
		if err := amqp.PublishTaskCreated(h.rabbitCh, h.queueName, created.ID); err != nil {
			h.logger.Warn("failed to publish event", zap.String("request_id", reqID), zap.Error(err))
		} else {
			h.logger.Info("event published", zap.String("request_id", reqID), zap.String("task_id", created.ID))
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(created)
}

// GetAllTasks
func (h *TaskHandler) GetAllTasks(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.GetRequestID(r.Context())
	if r.Method != http.MethodGet {
		h.logger.Warn("method not allowed", zap.String("request_id", reqID), zap.String("method", r.Method))
		h.handleError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	valid, _, statusCode := h.verifyToken(r)
	if !valid {
		h.handleError(w, statusCode, "unauthorized")
		return
	}

	tasks := h.taskSvc.GetAll()
	h.logger.Info("tasks listed", zap.String("request_id", reqID), zap.Int("count", len(tasks)))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tasks)
}

// GetTaskByID
func (h *TaskHandler) GetTaskByID(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.GetRequestID(r.Context())
	if r.Method != http.MethodGet {
		h.logger.Warn("method not allowed", zap.String("request_id", reqID), zap.String("method", r.Method))
		h.handleError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	valid, _, statusCode := h.verifyToken(r)
	if !valid {
		h.handleError(w, statusCode, "unauthorized")
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/v1/tasks/")
	if id == "" {
		h.logger.Warn("missing task id", zap.String("request_id", reqID))
		h.handleError(w, http.StatusBadRequest, "missing id")
		return
	}

	task, ok := h.taskSvc.GetByID(id)
	if !ok {
		h.logger.Info("task not found", zap.String("request_id", reqID), zap.String("task_id", id))
		h.handleError(w, http.StatusNotFound, "task not found")
		return
	}

	h.logger.Info("task retrieved", zap.String("request_id", reqID), zap.String("task_id", id))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(task)
}

// UpdateTask
func (h *TaskHandler) UpdateTask(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.GetRequestID(r.Context())
	if r.Method != http.MethodPatch {
		h.logger.Warn("method not allowed", zap.String("request_id", reqID), zap.String("method", r.Method))
		h.handleError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	valid, _, statusCode := h.verifyToken(r)
	if !valid {
		h.handleError(w, statusCode, "unauthorized")
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/v1/tasks/")
	if id == "" {
		h.logger.Warn("missing task id", zap.String("request_id", reqID))
		h.handleError(w, http.StatusBadRequest, "missing id")
		return
	}

	var updates service.Task
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		h.logger.Warn("invalid update body", zap.String("request_id", reqID), zap.Error(err))
		h.handleError(w, http.StatusBadRequest, "invalid json")
		return
	}

	updated, ok := h.taskSvc.Update(id, updates)
	if !ok {
		h.logger.Info("task not found for update", zap.String("request_id", reqID), zap.String("task_id", id))
		h.handleError(w, http.StatusNotFound, "task not found")
		return
	}

	h.logger.Info("task updated", zap.String("request_id", reqID), zap.String("task_id", id))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(updated)
}

// DeleteTask
func (h *TaskHandler) DeleteTask(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.GetRequestID(r.Context())
	if r.Method != http.MethodDelete {
		h.logger.Warn("method not allowed", zap.String("request_id", reqID), zap.String("method", r.Method))
		h.handleError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	valid, _, statusCode := h.verifyToken(r)
	if !valid {
		h.handleError(w, statusCode, "unauthorized")
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/v1/tasks/")
	if id == "" {
		h.logger.Warn("missing task id", zap.String("request_id", reqID))
		h.handleError(w, http.StatusBadRequest, "missing id")
		return
	}

	ok := h.taskSvc.Delete(id)
	if !ok {
		h.logger.Info("task not found for delete", zap.String("request_id", reqID), zap.String("task_id", id))
		h.handleError(w, http.StatusNotFound, "task not found")
		return
	}

	h.logger.Info("task deleted", zap.String("request_id", reqID), zap.String("task_id", id))
	w.WriteHeader(http.StatusNoContent)
}
