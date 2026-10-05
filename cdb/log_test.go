package cdb

import (
	"context"
	"testing"
)

func TestImpersonatorContext(t *testing.T) {
	if got := ImpersonatorFrom(context.Background()); got != "" {
		t.Errorf("no impersonation: %q", got)
	}
	ctx := WithImpersonator(context.Background(), "admin@example.com")
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	// The handlers derive their contexts from the request's: the mark follows.
	if got := ImpersonatorFrom(child); got != "admin@example.com" {
		t.Errorf("impersonator: %q", got)
	}
}
