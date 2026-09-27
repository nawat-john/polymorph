// Package shutdown provides graceful shutdown helpers shared by every
// service (design-plan.md sections 4, 11).
package shutdown

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// NewContext returns a context that is canceled on SIGINT or SIGTERM, and the
// stop function to release the signal notification early (e.g. via defer).
func NewContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
