package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ONSdigital/dp-kafka/v3/examples/producer/config"
	"github.com/ONSdigital/dp-kafka/v3/examples/producer/service"
	"github.com/ONSdigital/log.go/v2/log"
)

const serviceName = "kafka-example-producer"

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
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	go func() {
		sig := <-signals
		log.Warn(ctx, "[KAFKA-TEST] os signal received", log.Data{"signal": sig})
		cancel()
	}()

	// Read config
	cfg, err := config.Get()
	if err != nil {
		return fmt.Errorf("unable to retrieve configuration: %w", err)
	}

	// init and start service, which contains the kafka consumer
	svc := service.Service{}
	if err := svc.Init(ctx, cfg); err != nil {
		return fmt.Errorf("error initialising service: %w", err)
	}
	if err := svc.Start(ctx, cancel); err != nil {
		return fmt.Errorf("error starting service: %w", err)
	}

	// blocks until an os interrupt or a fatal error occurs
	sig := <-signals

	// graceful shutdown
	log.Info(ctx, "[KAFKA-TEST] os signal received", log.Data{"signal": sig})
	if err := svc.Close(ctx); err != nil {
		return fmt.Errorf("error closing service: %w", err)
	}
	return nil
}
