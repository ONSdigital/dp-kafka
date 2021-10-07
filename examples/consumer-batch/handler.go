package main

import (
	"context"
	"time"

	"github.com/ONSdigital/dp-kafka/v3/message"
	"github.com/ONSdigital/log.go/v2/log"
)

type Handler struct {
	cfg *Config
}

func (h *Handler) handleBatch(ctx context.Context, batch []message.Message) error {
	consumeCount += len(batch)
	msgDataByOffset := map[int64]string{}
	for _, msg := range batch {
		msgDataByOffset[msg.Offset()] = string(msg.GetData())
	}
	logData := log.Data{
		"consumeCount":       consumeCount,
		"messages_by_offset": msgDataByOffset}
	log.Info(ctx, "[KAFKA-TEST] Received batch", logData)

	// Allows us to dictate the process for shutting down and how fast we consume messages in this example app, (should not be used in applications)
	sleepIfRequired(ctx, h.cfg, logData)
	return nil
}

// sleepIfRequired sleeps if config requires to do so, in order to simulate a delay.
// Snooze will cause a delay of 500ms, and OverSleep will cause a delay of the timeout plus 500 ms.
// This function is for testing purposes only and should not be used in applications.
func sleepIfRequired(ctx context.Context, cfg *Config, logData log.Data) {
	var sleep time.Duration
	if cfg.Snooze || cfg.OverSleep {
		// Snooze slows consumption for testing
		sleep = 500 * time.Millisecond
		if cfg.OverSleep {
			// OverSleep tests taking more than shutdown timeout to process a message
			sleep += cfg.GracefulShutdownTimeout + time.Second*2
		}
		logData["sleep"] = sleep
	}

	log.Info(ctx, "[KAFKA-TEST] Message consumed", logData)
	if sleep > time.Duration(0) {
		time.Sleep(sleep)
		log.Info(ctx, "[KAFKA-TEST] done sleeping")
	}
}
