package handler

import (
	"context"
	"time"

	"github.com/ONSdigital/dp-kafka/v3/examples/consumer-batch/config"
	"github.com/ONSdigital/dp-kafka/v3/message"
	"github.com/ONSdigital/log.go/v2/log"
)

// consumeCount keeps track of the total number of consumed messages
var consumeCount = 0

type Handler struct {
	Cfg *config.Config
}

func (h *Handler) Handle(ctx context.Context, batch []message.Message) error {
	consumeCount += len(batch)
	msgDataByOffset := map[int64]string{}
	for _, msg := range batch {
		msgDataByOffset[msg.Offset()] = string(msg.GetData())
	}
	sleepTime := getSleepTime(h.Cfg)
	logData := log.Data{
		"consumeCount":       consumeCount,
		"sleep":              sleepTime,
		"messages_by_offset": msgDataByOffset}
	log.Info(ctx, "[KAFKA-TEST] Received batch", logData)
	if sleepTime > time.Duration(0) {
		time.Sleep(sleepTime)
	}
	log.Info(ctx, "[KAFKA-TEST] Batch processed", logData)
	return nil
}

// getSleepTime gets the time duration to sleeps, in order to simulate a message processing time.
// Snooze will cause a delay of 500ms, and OverSleep will cause a delay of the timeout plus 500 ms.
// This function is for testing purposes only and should not be used in applications.
func getSleepTime(cfg *config.Config) time.Duration {
	var sleep time.Duration
	if cfg.Snooze {
		// Snooze slows consumption for testing
		sleep = 500 * time.Millisecond
	}
	if cfg.OverSleep {
		// OverSleep tests taking more than shutdown timeout to process a message
		sleep += cfg.GracefulShutdownTimeout + time.Second*2
	}
	return sleep
}
