// Package main is the entrypoint for vector-k8s-helper.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sgl/vector-k8s-helper/internal/config"
	"github.com/sgl/vector-k8s-helper/internal/discovery"
	"github.com/sgl/vector-k8s-helper/internal/renderer"
	"github.com/sgl/vector-k8s-helper/internal/writer"
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
		"cluster_label", cfg.ClusterLabel,
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

	w := discovery.NewWatcher(client, cfg, logger)
	wr := writer.NewWriter(client, cfg.Namespace, cfg.ConfigMapKey, logger)

	rendCfg := renderer.Config{
		ScrapeIntervalSecs: cfg.ScrapeInterval.Seconds(),
		ClusterLabel:       cfg.ClusterLabel,
		AdditionalLabels:   cfg.AdditionalLabels,
	}

	go func() {
		if err := w.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Error("watcher stopped with error", "error", err)
			stop()
		}
	}()

	// Wait for initial target list, then render and write.
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
