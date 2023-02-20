package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ONSdigital/log.go/v2/log"
)

type DrainTopicInput struct {
	Topic     string
	GroupName string
}

type DrainTopicConfig struct {
	BrokerAddrs  []string
	BatchSize    int
	Timeout      time.Duration
	KafkaVersion *string
}

// DrainTopics drains the provided topics in parallel.
// Each topic-group pair will create a new kafka consumer and conusume all the kafka messages in the topic for the provided group.
// Onece all queues for all provided topics and gropus are empty, this function will return.
func DrainTopics(ctx context.Context, cfg *DrainTopicConfig, in ...*DrainTopicInput) {
	wg := &sync.WaitGroup{}

	for _, input := range in {
		if err := DrainTopic(ctx, cfg, input, wg); err != nil {
			log.Error(ctx, "error draining topic", err, log.Data{
				"topic": input.Topic,
				"group": input.GroupName,
			})
		}
	}

	wg.Wait()
}

// DrainTopic drains the provided topic and group of any residual messages.
// This might be useful to clean up environments, or component tests,
// preventing future tests failing if previous tests fail unexpectedly and
// leave messages in the queue.
//
// A temporary batch consumer is used, that is created and closed within this func
// A maximum of DrainTopicMaxMessages messages will be drained from the provided topic and group.
//
// This method accepts a waitGroup pionter. If it is not nil, it will wait for the topic to be drained
// in a new go-routine, which will be added to the waitgroup. If it is nil, execution will be blocked
// until the topic is drained (or time out expires)
func DrainTopic(ctx context.Context, cfg *DrainTopicConfig, in *DrainTopicInput, wg *sync.WaitGroup) error {
	if in == nil {
		return errors.New("invalid drain topic input")
	}
	if cfg == nil {
		return errors.New("invalid drain topic config")
	}

	kafkaOffset := OffsetOldest
	drainer, err := NewConsumerGroup(
		ctx,
		&ConsumerGroupConfig{
			BrokerAddrs:   cfg.BrokerAddrs,
			Topic:         in.Topic,
			GroupName:     in.GroupName,
			KafkaVersion:  cfg.KafkaVersion,
			Offset:        &kafkaOffset,
			BatchSize:     &cfg.BatchSize,
			BatchWaitTime: &cfg.Timeout,
		},
	)
	if err != nil {
		return fmt.Errorf("error creating kafka consumer to drain topic: %w", err)
	}

	// register batch handler with 'drainer' consumer
	drained := make(chan struct{})
	msgs := []Message{}
	if err := drainer.RegisterBatchHandler(
		ctx,
		func(ctx context.Context, batch []Message) error {
			defer close(drained)
			msgs = append(msgs, batch...)
			return nil
		},
	); err != nil {
		return fmt.Errorf("error creating kafka drainer: %w", err)
	}

	// start kafka logging go-routines
	drainer.LogErrors(ctx)

	// start drainer consumer group
	if err := drainer.Start(); err != nil {
		log.Error(ctx, "error starting kafka drainer", err)
	}

	// waitUntilDrained is a func that will wait until the batch is consumed or the timeout expires
	// (with 100 ms of extra time to allow any in-flight drain)
	waitUntilDrained := func() {
		drainer.StateWait(Consuming)
		log.Info(ctx, "drainer is consuming", log.Data{"topic": in.Topic, "group": in.GroupName})

		select {
		case <-time.After(cfg.Timeout + 100*time.Millisecond):
			log.Info(ctx, "drain timeout has expired (no messages drained)")
		case <-drained:
			log.Info(ctx, "message(s) have been drained")
		}

		defer func() {
			log.Info(ctx, "drained topic", log.Data{
				"len":      len(msgs),
				"messages": msgs,
				"topic":    in.Topic,
				"group":    in.GroupName,
			})
		}()

		if err := drainer.Close(ctx); err != nil {
			log.Warn(ctx, "error closing drain consumer", log.Data{"err": err})
		}

		<-drainer.Channels().Closed
		log.Info(ctx, "drainer is closed")
	}

	// sync wait if wg is not provided
	if wg == nil {
		waitUntilDrained()
		return nil
	}

	// async wait if wg is provided
	wg.Add(1)
	go func() {
		defer wg.Done()
		waitUntilDrained()
	}()
	return nil
}
