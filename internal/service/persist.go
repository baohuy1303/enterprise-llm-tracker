package service

import (
	"context"
	"log/slog"

	"enterprise-llm-tracker/internal/store"
)

type PersistService struct {
	store  *store.Store
	logger *slog.Logger
}

func NewPersistService(st *store.Store, logger *slog.Logger) *PersistService {
	if logger == nil {
		logger = slog.Default()
	}
	return &PersistService{store: st, logger: logger}
}

// HandleEvent inserts the event into usage_events. Returns the underlying
// error so the Kafka consumer leaves the offset uncommitted on failure — the
// message will be redelivered. Inserts are idempotent on event_id, so replay
// is safe.
func (p *PersistService) HandleEvent(ctx context.Context, e store.Event) error {
	return p.store.WriteEventPG(ctx, e)
}
