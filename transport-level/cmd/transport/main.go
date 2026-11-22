// Command transport runs the transport level (Variant 6): it forwards document topics to the agent,
// passes structure segments through Kafka with simulated 6% loss, and delivers reassembled structures to the app.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/dima040805/transport-level/internal/assembler"
	"github.com/dima040805/transport-level/internal/clients"
	"github.com/dima040805/transport-level/internal/collector"
	"github.com/dima040805/transport-level/internal/config"
	"github.com/dima040805/transport-level/internal/httpapi"
	"github.com/dima040805/transport-level/internal/kafkaio"
	"github.com/dima040805/transport-level/internal/segment"
	"github.com/dima040805/transport-level/internal/senders"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("transport stopped", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	logger.Info("starting transport level",
		"listen", cfg.ListenAddr,
		"kafka", cfg.KafkaBroker,
		"agent", cfg.AgentURL,
		"app", cfg.AppURL,
		"loss_percent", cfg.SegmentLossPercent,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	httpClient := &http.Client{Timeout: cfg.HTTPClientTimeout}
	publisher := kafkaio.NewPublisher(cfg.KafkaBroker, cfg.KafkaTopic)
	defer publisher.Close()

	registry := senders.NewRegistry()
	asm := assembler.New(cfg.SegmentTimeout)
	coll := collector.New(asm, registry, clients.NewApp(cfg.AppURL, httpClient), cfg.BufferCheckInterval, logger)

	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		kafkaio.Consume(ctx, cfg.KafkaBroker, cfg.KafkaTopic, cfg.KafkaGroupID, logger, func(s segment.Segment) {
			asm.Add(s, time.Now())
		})
	}()
	go func() {
		defer workers.Done()
		coll.Run(ctx)
	}()

	api := httpapi.New(clients.NewAgent(cfg.AgentURL, httpClient), publisher, registry, cfg.SegmentLossPercent, logger)
	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()

	select {
	case err := <-serveErr:
		stop()
		workers.Wait()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	workers.Wait()
	return err
}