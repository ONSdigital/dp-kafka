package main

import (
	"bufio"
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/ONSdigital/dp-healthcheck/healthcheck"
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

// period of time between tickers
const ticker = 1 * time.Second

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

	ctx := context.Background()

	log.Event(ctx, "[KAFKA-TEST] Starting (consumer sent to stdout, stdin sent to producer)",
		log.Data{"consumed_group": cfg.ConsumedGroup, "consumed_topic": cfg.ConsumedTopic, "produced_topic": cfg.ProducedTopic})

	kafka.SetMaxMessageSize(int32(cfg.KafkaMaxBytes))

	// Create Producer with channels
	pChannels := kafka.CreateProducerChannels()
	producer, err := kafka.NewProducer(
		ctx, cfg.Brokers, cfg.ProducedTopic, cfg.KafkaMaxBytes, pChannels)
	if err != nil {
		log.Event(ctx, "[KAFKA-TEST] Fatal error creating producer.", log.Error(err))
		os.Exit(1)
	}
	if !producer.IsInitialised() {
		log.Event(ctx, "[KAFKA-TEST] Producer could not be initialised at creation time. Please, try to initialise it later.")
	}

	// Create Consumer with channels
	cgChannels := kafka.CreateConsumerGroupChannels(cfg.KafkaSync)
	consumer, err := kafka.NewConsumerGroup(
		ctx, cfg.Brokers, cfg.ConsumedTopic, cfg.ConsumedGroup, kafka.OffsetNewest, cfg.KafkaSync, cgChannels)
	if err != nil {
		log.Event(ctx, "[KAFKA-TEST] Fatal error creating consumer.", log.Error(err))
		os.Exit(1)
	}
	if !producer.IsInitialised() {
		log.Event(ctx, "[KAFKA-TEST] Consumer could not be initialised at creation time. Please, try to initialise it later.")
	}

	// Create signals and stdin channels
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	stdinChannel := make(chan string)

	// Create health check
	versionInfo, err := healthcheck.NewVersionInfo(
		"1580818588",
		"myGitCommit",
		"myVersion",
	)
	if err != nil {
		log.Event(ctx, "failed to create service version information", log.Error(err))
		os.Exit(1)
	}
	hc := healthcheck.New(versionInfo, 1*time.Minute, 10*time.Second)
	hc.AddCheck(kafka.ServiceName, producer.Checker)
	hc.AddCheck(kafka.ServiceName, consumer.Checker)

	// go-routines to log errors from error channels
	cgChannels.LogErrors(ctx, "[KAFKA-TEST] Consumer error")
	pChannels.LogErrors(ctx, "[KAFKA-TEST] Producer error")

	// Create loop-control channel and context
	eventLoopContext, eventLoopCancel := context.WithCancel(context.Background())
	eventLoopDone := make(chan bool)
	consumeCount := 0

	// stdin reader loop
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

			case <-time.After(ticker):
				log.Event(ctx, "[KAFKA-TEST] tick")

			case <-eventLoopContext.Done():
				log.Event(ctx, "[KAFKA-TEST] Event loop context done", log.Data{"eventLoopContextErr": eventLoopContext.Err()})
				return

			case consumedMessage := <-cgChannels.Upstream:
				// consumer will be nil if the broker could not be contacted, that's why we use the channel directly instead of consumer.Incoming()
				consumeCount++
				logData := log.Data{"consumeCount": consumeCount, "consumeMax": cfg.ConsumeMax, "messageOffset": consumedMessage.Offset()}
				log.Event(ctx, "[KAFKA-TEST] Received message", logData)

				consumedData := consumedMessage.GetData()
				logData["messageString"] = string(consumedData)
				logData["messageRaw"] = consumedData
				logData["messageLen"] = len(consumedData)

				// Allows us to dictate the process for shutting down and how fast we consume messages in this example app, (should not be used in applications)
				sleepIfRequired(ctx, cfg, logData)

				// send downstream
				pChannels.Output <- consumedData

				if cfg.KafkaSync {
					log.Event(ctx, "[KAFKA-TEST] pre-release")
					consumer.CommitAndRelease(consumedMessage)
				} else {
					log.Event(ctx, "[KAFKA-TEST] pre-commit")
					consumedMessage.Commit()
				}
				log.Event(ctx, "[KAFKA-TEST] committed message", log.Data{"messageOffset": consumedMessage.Offset()})
				if consumeCount == cfg.ConsumeMax {
					log.Event(ctx, "[KAFKA-TEST] consumed max - exiting eventLoop", nil)
					return
				}

			case stdinLine := <-stdinChannel:
				// Used for this example to write messages to kafka consumer topic (should not be needed in applications)
				pChannels.Output <- []byte(stdinLine)
				log.Event(ctx, "[KAFKA-TEST] Message output", log.Data{"messageSent": stdinLine, "messageChars": []byte(stdinLine)})
			}
		}
	}()

	// Start Healthcheck
	log.Event(ctx, "[KAFKA-TEST] Starting health-check")
	hc.Start(ctx)

	// block until a fatal error, signal or eventLoopDone - then proceed to shutdown
	select {
	case <-eventLoopDone:
		log.Event(ctx, "[KAFKA-TEST] Quitting after event loop aborted")
	case sig := <-signals:
		log.Event(ctx, "[KAFKA-TEST] Quitting after OS signal", log.Data{"signal": sig})
	}

	// give the app `Timeout` seconds to close gracefully before killing it.
	ctx, cancel := context.WithTimeout(context.Background(), cfg.TimeOut)

	// background graceful shutdown
	go func() {
		log.Event(ctx, "[KAFKA-TEST] Stopping health-check")
		hc.Stop()
		log.Event(ctx, "[KAFKA-TEST] Stopping kafka consumer listener")
		consumer.StopListeningToConsumer(ctx)
		log.Event(ctx, "[KAFKA-TEST] Stopped kafka consumer listener")
		eventLoopCancel()
		// wait for eventLoopDone: all in-flight messages have been processed
		<-eventLoopDone
		log.Event(ctx, "[KAFKA-TEST] Closing kafka producer")
		producer.Close(ctx)
		log.Event(ctx, "[KAFKA-TEST] Closed kafka producer")
		log.Event(ctx, "[KAFKA-TEST] Closing kafka consumer")
		consumer.Close(ctx)
		log.Event(ctx, "[KAFKA-TEST] Closed kafka consumer")

		log.Event(ctx, "[KAFKA-TEST] Done shutdown - cancelling timeout context")
		cancel() // stop timer
	}()

	// wait for timeout or success (via cancel)
	<-ctx.Done()
	if ctx.Err() == context.DeadlineExceeded {
		log.Event(ctx, "[KAFKA-TEST]", log.Error(ctx.Err()))
	} else {
		log.Event(ctx, "[KAFKA-TEST] Done shutdown gracefully", log.Data{"context": ctx.Err()})
	}
	os.Exit(1)
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
			sleep += cfg.TimeOut
		}
		logData["sleep"] = sleep
	}

	log.Event(ctx, "[KAFKA-TEST] Message consumed", logData)
	if sleep > time.Duration(0) {
		time.Sleep(sleep)
		log.Event(ctx, "[KAFKA-TEST] done sleeping")
	}
}
