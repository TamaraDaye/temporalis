package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/TamaraDaye/temporalis/internal/controlplane"
)

func main() {
	cfg := controlplane.DefaultConfig()

	flag.StringVar(&cfg.Application, "app", cfg.Application, "application name")
	flag.StringVar(&cfg.HTTPAddr, "http", cfg.HTTPAddr, "HTTP registration API listen address")
	flag.StringVar(&cfg.SubscriptionAddr, "subscription", cfg.SubscriptionAddr, "gateway subscription listen address")
	flag.DurationVar(&cfg.HeartbeatTTL, "heartbeat-ttl", cfg.HeartbeatTTL, "time without a heartbeat before an instance is reaped")
	flag.DurationVar(&cfg.ReapInterval, "reap-interval", cfg.ReapInterval, "how often to scan for expired instances")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("starting control plane %q: http=%s subscription=%s", cfg.Application, cfg.HTTPAddr, cfg.SubscriptionAddr)
	if err := controlplane.NewServer(cfg).Run(ctx); err != nil {
		log.Fatal(err)
	}
}
