// Package main is the entrypoint for vector-k8s-helper.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sonroyaalmerol/vector-k8s-helper/internal/config"
	"github.com/sonroyaalmerol/vector-k8s-helper/internal/discovery"
	"github.com/sonroyaalmerol/vector-k8s-helper/internal/renderer"
	"github.com/sonroyaalmerol/vector-k8s-helper/internal/writer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func main() {
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	logger.Info("configuration loaded",
		"namespace", cfg.Namespace,
		"configmap", cfg.ConfigMapName,
		"scrape_interval", cfg.ScrapeInterval,
		"scrape_timeout", cfg.ScrapeTimeout,
		"cluster_label", cfg.ClusterLabel,
		"honor_labels", cfg.HonorLabels,
	)

	k8sCfg, err := rest.InClusterConfig()
	if err != nil {
		logger.Error("failed to get in-cluster config", "error", err)
		os.Exit(1)
	}

	client, err := kubernetes.NewForConfig(k8sCfg)
	if err != nil {
		logger.Error("failed to create k8s client", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Start health/metrics server.
	go serveHealth(ctx, cfg.MetricsAddr, logger)

	w := discovery.NewWatcher(client, cfg, logger)
	wr := writer.NewWriter(client, cfg.Namespace, cfg.ConfigMapKey, logger)

	rendCfg := renderer.Config{
		ScrapeIntervalSecs: cfg.ScrapeInterval.Seconds(),
		ScrapeTimeoutSecs:  cfg.ScrapeTimeout.Seconds(),
		HonorLabels:        cfg.HonorLabels,
		ClusterLabel:       cfg.ClusterLabel,
		AdditionalLabels:   cfg.AdditionalLabels,
	}

	go func() {
		if err := w.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Error("watcher stopped with error", "error", err)
			stop()
		}
	}()

	var lastContent []byte
	for targets := range w.Output() {
		logger.Info("targets changed", "count", len(targets))

		content, err := renderer.Render(targets, rendCfg)
		if err != nil {
			logger.Error("failed to render config", "error", err)
			continue
		}

		if string(content) == string(lastContent) {
			logger.Debug("config unchanged, skipping write")
			continue
		}
		lastContent = content

		writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if err := wr.Upsert(writeCtx, cfg.ConfigMapName, content); err != nil {
			logger.Error("failed to write configmap", "error", err)
		}
		cancel()
	}

	logger.Info("shutting down")
}

// serveHealth starts a simple HTTP server for health checks.
// The /health endpoint responds with 200 OK, allowing the helper
// pod to be annotated with prometheus.io/scrape for self-monitoring.
func serveHealth(ctx context.Context, addr string, logger *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		logger.Info("health server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && ctx.Err() == nil {
			logger.Error("health server error", "error", err)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("health server shutdown error", "error", err)
	}
}

// buildVersion is set at build time via -ldflags.
var buildVersion = "dev"

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "vector-k8s-helper %s\n\nUsage:\n", buildVersion)
		flag.PrintDefaults()
	}
}
