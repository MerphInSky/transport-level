// Package collector periodically delivers reassembled messages to the application level.
package collector

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/dima040805/transport-level/internal/assembler"
	"github.com/dima040805/transport-level/internal/clients"
	"github.com/dima040805/transport-level/internal/senders"
)

// App receives reassembled messages.
type App interface {
	Deliver(ctx context.Context, req clients.ReceiveRequest) error
}

type Collector struct {
	assembler *assembler.Assembler
	senders   *senders.Registry
	app       App
	interval  time.Duration
	logger    *slog.Logger
}

func New(asm *assembler.Assembler, registry *senders.Registry, app App, interval time.Duration, logger *slog.Logger) *Collector {
	return &Collector{assembler: asm, senders: registry, app: app, interval: interval, logger: logger}
}

// Run checks the buffer every interval until ctx is cancelled, then waits for
// deliveries that are already in flight.
func (c *Collector) Run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	var inflight sync.WaitGroup
	defer inflight.Wait()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, msg := range c.assembler.Collect(time.Now()) {
				inflight.Add(1)
				go func() {
					defer inflight.Done()
					c.deliver(context.WithoutCancel(ctx), msg)
				}()
			}
		}
	}
}

// DeliverReady synchronously delivers every ready message and returns how many there were.
func (c *Collector) DeliverReady(ctx context.Context) int {
	msgs := c.assembler.Collect(time.Now())
	for _, msg := range msgs {
		c.deliver(ctx, msg)
	}
	return len(msgs)
}

func (c *Collector) deliver(ctx context.Context, msg assembler.Message) {
	var payload *string
	if !msg.Incomplete {
		p := msg.Payload
		payload = &p
	}

	req := clients.ReceiveRequest{
		Sender:    c.senders.Take(msg.Timestamp),
		Timestamp: msg.Timestamp,
		Error:     msg.Incomplete,
		Payload:   payload,
	}
	if err := c.app.Deliver(ctx, req); err != nil {
		c.logger.Error("deliver: failed", "timestamp", msg.Timestamp, "err", err)
		return
	}
	c.logger.Info("deliver: ok",
		"timestamp", msg.Timestamp,
		"sender", req.Sender,
		"received", msg.Received,
		"total", msg.Total,
		"error", msg.Incomplete,
	)
}