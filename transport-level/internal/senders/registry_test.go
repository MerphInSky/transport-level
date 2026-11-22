package senders

import "testing"

func TestTakeForgetsSender(t *testing.T) {
	r := NewRegistry()
	r.Remember(1715600000, "Ivan_Student")

	if got := r.Take(1715600000); got != "Ivan_Student" {
		t.Fatalf("Take = %q, want Ivan_Student", got)
	}
	if got := r.Take(1715600000); got != "" {
		t.Fatalf("second Take = %q, want empty", got)
	}
}