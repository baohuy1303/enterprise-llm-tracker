package http

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"enterprise-llm-tracker/internal/service"
	"enterprise-llm-tracker/internal/store"
)

// EngineerHandlers implements REST CRUD for /admin/engineers. Kept thin —
// validation, persistence, and Redis joins live in service.EngineerService.
type EngineerHandlers struct {
	svc    *service.EngineerService
	logger *slog.Logger
}

func NewEngineerHandlers(svc *service.EngineerService, logger *slog.Logger) *EngineerHandlers {
	if logger == nil {
		logger = slog.Default()
	}
	return &EngineerHandlers{svc: svc, logger: logger}
}

// createEngineerRequest is the wire format for POST /admin/engineers.
// Decoupled from store.EngineerCreate so we can rename internal fields without
// breaking clients.
type createEngineerRequest struct {
	Email            string  `json:"email"`
	Name             string  `json:"name"`
	GitHubUsername   string  `json:"github_username"`
	SlackUserID      string  `json:"slack_user_id,omitempty"`
	ManagerSlackID   string  `json:"manager_slack_id,omitempty"`
	DailyBudgetUSD   float64 `json:"daily_budget_usd,omitempty"`
	MonthlyBudgetUSD float64 `json:"monthly_budget_usd,omitempty"`
	Team             string  `json:"team,omitempty"`
}

// updateEngineerRequest uses pointer fields so JSON omission means "leave
// untouched" while an explicit null/value means "set to this".
type updateEngineerRequest struct {
	Name             *string  `json:"name,omitempty"`
	GitHubUsername   *string  `json:"github_username,omitempty"`
	SlackUserID      *string  `json:"slack_user_id,omitempty"`
	ManagerSlackID   *string  `json:"manager_slack_id,omitempty"`
	DailyBudgetUSD   *float64 `json:"daily_budget_usd,omitempty"`
	MonthlyBudgetUSD *float64 `json:"monthly_budget_usd,omitempty"`
	Team             *string  `json:"team,omitempty"`
}

// Create handles POST /admin/engineers.
func (h *EngineerHandlers) Create(w http.ResponseWriter, r *http.Request) {
	var req createEngineerRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	eng, err := h.svc.Create(r.Context(), store.EngineerCreate{
		Email:            req.Email,
		Name:             req.Name,
		GitHubUsername:   req.GitHubUsername,
		SlackUserID:      req.SlackUserID,
		ManagerSlackID:   req.ManagerSlackID,
		DailyBudgetUSD:   req.DailyBudgetUSD,
		MonthlyBudgetUSD: req.MonthlyBudgetUSD,
		Team:             req.Team,
	})
	if err != nil {
		h.writeServiceErr(w, err, "create engineer")
		return
	}
	writeJSON(w, http.StatusCreated, eng)
}

// List handles GET /admin/engineers.
func (h *EngineerHandlers) List(w http.ResponseWriter, r *http.Request) {
	out, err := h.svc.List(r.Context())
	if err != nil {
		h.writeServiceErr(w, err, "list engineers")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"engineers": out,
		"count":     len(out),
	})
}

// Get handles GET /admin/engineers/{email}. Path-value extraction uses Go
// 1.22+ mux routing — see router.go for the route registration.
func (h *EngineerHandlers) Get(w http.ResponseWriter, r *http.Request) {
	email := r.PathValue("email")
	if email == "" {
		writeError(w, http.StatusBadRequest, "email path parameter required")
		return
	}
	detail, err := h.svc.Get(r.Context(), email)
	if err != nil {
		h.writeServiceErr(w, err, "get engineer")
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

// Update handles PUT /admin/engineers/{email}.
func (h *EngineerHandlers) Update(w http.ResponseWriter, r *http.Request) {
	email := r.PathValue("email")
	if email == "" {
		writeError(w, http.StatusBadRequest, "email path parameter required")
		return
	}
	var req updateEngineerRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	eng, err := h.svc.Update(r.Context(), email, store.EngineerUpdate{
		Name:             req.Name,
		GitHubUsername:   req.GitHubUsername,
		SlackUserID:      req.SlackUserID,
		ManagerSlackID:   req.ManagerSlackID,
		DailyBudgetUSD:   req.DailyBudgetUSD,
		MonthlyBudgetUSD: req.MonthlyBudgetUSD,
		Team:             req.Team,
	})
	if err != nil {
		h.writeServiceErr(w, err, "update engineer")
		return
	}
	writeJSON(w, http.StatusOK, eng)
}

// Delete handles DELETE /admin/engineers/{email} as a soft delete.
func (h *EngineerHandlers) Delete(w http.ResponseWriter, r *http.Request) {
	email := r.PathValue("email")
	if email == "" {
		writeError(w, http.StatusBadRequest, "email path parameter required")
		return
	}
	if err := h.svc.Deactivate(r.Context(), email); err != nil {
		h.writeServiceErr(w, err, "deactivate engineer")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// writeServiceErr maps service/store errors to HTTP status codes. Validation
// errors are 400, not-found is 404, unique conflicts are 409, everything else
// is 500 with the error logged but not surfaced to the client.
func (h *EngineerHandlers) writeServiceErr(w http.ResponseWriter, err error, op string) {
	switch {
	case errors.Is(err, store.ErrEngineerNotFound):
		writeError(w, http.StatusNotFound, "engineer not found")
	case errors.Is(err, store.ErrEngineerExists):
		writeError(w, http.StatusConflict, "engineer with that email or github_username already exists")
	case service.IsValidation(err):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		h.logger.Error(op+" failed", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

// --- shared JSON helpers (used by other admin handlers too) ----------------

func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
