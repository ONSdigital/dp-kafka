package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ONSdigital/dp-kafka/v3/examples/consumer-batch/config"
	"github.com/ONSdigital/dp-kafka/v3/examples/consumer-batch/service"
	"github.com/ONSdigital/log.go/v2/log"
)

const serviceName = "kafka-example-consumer"

func main() {
	log.Namespace = serviceName
	ctx := context.Background()

	if err := run(ctx); err != nil {
		log.Fatal(ctx, "fatal runtime error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Read config
	cfg, err := config.Get()
	if err != nil {
		return fmt.Errorf("unable to retrieve configuration: %w", err)
	}
	log.Info(ctx, "config on startup", log.Data{"config": cfg})

	serviceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// init and start service, which contains the kafka consumer
	svc := service.Service{}
	if err := svc.Init(serviceCtx, cfg); err != nil {
		return fmt.Errorf("error initialising service: %w", err)
	}
	if err := svc.Start(serviceCtx); err != nil {
		return fmt.Errorf("error starting service: %w", err)
	}

	// blocks until an os interrupt or a fatal error occurs
	sig := <-signals

	// graceful shutdown
	log.Info(ctx, "[KAFKA-TEST] os signal received", log.Data{"signal": sig})
	if err := svc.Close(serviceCtx); err != nil {
		return fmt.Errorf("error closing service: %w", err)
	}
	return nil
}
