package main

import (
	"bufio"
	"context"
	"os"
	"os/signal"
	"time"

	kafka "github.com/ONSdigital/dp-kafka"
	"github.com/ONSdigital/log.go/log"
	"github.com/kelseyhightower/envconfig"
)

// Config is the kafka configuration for this example
type Config struct {
	Brokers       []string      `envconfig:"KAFKA_ADDR"`
	KafkaMaxBytes int           `envconfig:"KAFKA_MAX_BYTES"`
	ConsumedTopic string        `envconfig:"KAFKA_CONSUMED_TOPIC"`
	ConsumedGroup string        `envconfig:"KAFKA_CONSUMED_GROUP"`
	ProducedTopic string        `envconfig:"KAFKA_PRODUCED_TOPIC"`
	ConsumeMax    int           `envconfig:"KAFKA_CONSUME_MAX"`
	KafkaSync     bool          `envconfig:"KAFKA_SYNC"`
	TimeOut       time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	Chomp         bool          `envconfig:"CHOMP_MSG"`
	Snooze        bool          `envconfig:"SNOOZE"`
	OverSleep     bool          `envconfig:"OVERSLEEP"`
}

func main() {
	log.Namespace = "kafka-example"
	cfg := &Config{
		Brokers:       []string{"localhost:9092"},
		KafkaMaxBytes: 50 * 1024 * 1024,
		KafkaSync:     true,
		ConsumedGroup: log.Namespace,
		ConsumedTopic: "input",
		ProducedTopic: "output",
		ConsumeMax:    0,
		TimeOut:       5 * time.Second,
		Chomp:         false,
		Snooze:        false,
		OverSleep:     false,
	}
	if err := envconfig.Process("", cfg); err != nil {
		panic(err)
	}

	log.Event(nil, "[KAFKA-TEST] Starting (consumer sent to stdout, stdin sent to producer)",
		log.Data{"consumed_group": cfg.ConsumedGroup, "consumed_topic": cfg.ConsumedTopic, "produced_topic": cfg.ProducedTopic})

	kafka.SetMaxMessageSize(int32(cfg.KafkaMaxBytes))

	var (
		chOut    = make(chan []byte)
		chErr    = make(chan error)
		chCloser = make(chan struct{})
		chClosed = make(chan struct{})
	)
	producer, err := kafka.NewProducer(cfg.Brokers, cfg.ProducedTopic, cfg.KafkaMaxBytes, chOut, chErr, chCloser, chClosed)
	if err != nil {
		log.Event(nil, "[KAFKA-TEST] Could not create producer", log.Error(err))
		panic("[KAFKA-TEST] Could not create producer")
	}

	var consumer *kafka.ConsumerGroup
	if cfg.KafkaSync {
		consumer, err = kafka.NewSyncConsumer(cfg.Brokers, cfg.ConsumedTopic, cfg.ConsumedGroup, kafka.OffsetNewest)
	} else {
		consumer, err = kafka.NewConsumerGroup(cfg.Brokers, cfg.ConsumedTopic, cfg.ConsumedGroup, kafka.OffsetNewest)
	}
	if err != nil {
		log.Event(nil, "[KAFKA-TEST] Could not create consumer", log.Error(err))
		panic("[KAFKA-TEST] Could not create consumer")
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	stdinChannel := make(chan string)

	eventLoopContext, eventLoopCancel := context.WithCancel(context.Background())
	eventLoopDone := make(chan bool)
	consumeCount := 0

	go func(ch chan string) {
		reader := bufio.NewReader(os.Stdin)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				break
			}
			if cfg.Chomp {
				line = line[:len(line)-1]
			}
			ch <- line
		}
		eventLoopCancel()
		<-eventLoopDone
		close(ch)
	}(stdinChannel)

	// eventLoop
	go func() {
		defer close(eventLoopDone)
		for {
			select {
			case <-time.After(1 * time.Second):
				log.Event(nil, "[KAFKA-TEST] tick")
			case <-eventLoopContext.Done():
				log.Event(nil, "[KAFKA-TEST] Event loop context done", log.Data{"eventLoopContextErr": eventLoopContext.Err()})
				return
			case consumedMessage := <-consumer.Incoming():
				consumeCount++
				logData := log.Data{"consumeCount": consumeCount, "consumeMax": cfg.ConsumeMax, "messageOffset": consumedMessage.Offset()}
				log.Event(nil, "[KAFKA-TEST] Received message", logData)

				consumedData := consumedMessage.GetData()
				logData["messageString"] = string(consumedData)
				logData["messageRaw"] = consumedData
				logData["messageLen"] = len(consumedData)

				var sleep time.Duration
				if cfg.Snooze || cfg.OverSleep {
					// Snooze slows consumption for testing
					sleep = 500 * time.Millisecond
					if cfg.OverSleep {
						// OverSleep tests taking more than shutdown timeout to process a message
						sleep += cfg.TimeOut
					}
					logData["sleep"] = sleep
				}

				log.Event(nil, "[KAFKA-TEST] Message consumed", logData)
				if sleep > time.Duration(0) {
					time.Sleep(sleep)
					log.Event(nil, "[KAFKA-TEST] done sleeping")
				}

				// send downstream
				producer.Output() <- consumedData

				if cfg.KafkaSync {
					log.Event(nil, "[KAFKA-TEST] pre-release")
					consumer.CommitAndRelease(consumedMessage)
				} else {
					log.Event(nil, "[KAFKA-TEST] pre-commit")
					consumedMessage.Commit()
				}
				log.Event(nil, "[KAFKA-TEST] committed message", log.Data{"messageOffset": consumedMessage.Offset()})
				if consumeCount == cfg.ConsumeMax {
					log.Event(nil, "[KAFKA-TEST] consumed max - exiting eventLoop", nil)
					return
				}
			case stdinLine := <-stdinChannel:
				producer.Output() <- []byte(stdinLine)
				log.Event(nil, "[KAFKA-TEST] Message output", log.Data{"messageSent": stdinLine, "messageChars": []byte(stdinLine)})
			}
		}
	}()

	// block until a fatal error, signal or eventLoopDone - then proceed to shutdown
	select {
	case <-eventLoopDone:
		log.Event(nil, "[KAFKA-TEST] Quitting after event loop aborted")
	case sig := <-signals:
		log.Event(nil, "[KAFKA-TEST] Quitting after OS signal", log.Data{"signal": sig})
	case consumerError := <-consumer.Errors():
		log.Event(nil, "[KAFKA-TEST] Aborting consumer", log.Error(consumerError))
	case producerError := <-producer.Errors():
		log.Event(nil, "[KAFKA-TEST] Aborting producer", log.Error(producerError))
	}

	// give the app `Timeout` seconds to close gracefully before killing it.
	ctx, cancel := context.WithTimeout(context.Background(), cfg.TimeOut)

	// background graceful shutdown
	go func() {
		log.Event(nil, "[KAFKA-TEST] Stopping kafka consumer listener")
		consumer.StopListeningToConsumer(ctx)
		log.Event(nil, "[KAFKA-TEST] Stopped kafka consumer listener")
		eventLoopCancel()
		// wait for eventLoopDone: all in-flight messages have been processed
		<-eventLoopDone
		log.Event(nil, "[KAFKA-TEST] Closing kafka producer")
		producer.Close(ctx)
		log.Event(nil, "[KAFKA-TEST] Closed kafka producer")
		log.Event(nil, "[KAFKA-TEST] Closing kafka consumer")
		consumer.Close(ctx)
		log.Event(nil, "[KAFKA-TEST] Closed kafka consumer")

		log.Event(nil, "[KAFKA-TEST] Done shutdown - cancelling timeout context")
		cancel() // stop timer
	}()

	// wait for timeout or success (via cancel)
	<-ctx.Done()
	if ctx.Err() == context.DeadlineExceeded {
		log.Event(nil, "[KAFKA-TEST]", log.Error(ctx.Err()))
	} else {
		log.Event(nil, "[KAFKA-TEST] Done shutdown gracefully", log.Data{"context": ctx.Err()})
	}
	os.Exit(1)
}
