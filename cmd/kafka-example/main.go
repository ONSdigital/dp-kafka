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

// period of time between tickers
const ticker = 3 * time.Second

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

	// Create Producer with channels provided from caller
	var (
		chOut            = make(chan []byte)
		chProducerErr    = make(chan error)
		chProducerCloser = make(chan struct{})
		chProducerClosed = make(chan struct{})
	)
	producer, err := kafka.NewProducer(
		ctx, cfg.Brokers, cfg.ProducedTopic, cfg.KafkaMaxBytes,
		chOut, chProducerErr, chProducerCloser, chProducerClosed,
	)
	if err != nil {
		log.Event(ctx, "[KAFKA-TEST] Could not create producer. Please, try to reconnect later", log.Error(err))
	}

	// Create Consumer with channels provided from caller
	var (
		chUpstream       chan kafka.Message
		chConsumerCloser = make(chan struct{})
		chConsumerClosed = make(chan struct{})
		chConsumerErr    = make(chan error)
		chUpstreamDone   = make(chan bool, 1)
	)
	if cfg.KafkaSync {
		// Sync -> upstream channel buffered, so we can send-and-wait for upstreamDone
		chUpstream = make(chan kafka.Message, 1)
	} else {
		chUpstream = make(chan kafka.Message)
	}
	consumer, err := kafka.NewConsumerWithChannels(
		ctx, cfg.Brokers, cfg.ConsumedTopic, cfg.ConsumedGroup, kafka.OffsetNewest, cfg.KafkaSync,
		chUpstream, chConsumerCloser, chConsumerClosed, chConsumerErr, chUpstreamDone,
	)
	if err != nil {
		log.Event(ctx, "[KAFKA-TEST] Could not create consumer. Please try to reconnect later", log.Error(err))
	}

	// Create signals and stdin channels
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	stdinChannel := make(chan string)

	// Create loop-control channel and context
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
			case <-time.After(ticker):
				log.Event(ctx, "[KAFKA-TEST] tick")
			case <-eventLoopContext.Done():
				log.Event(ctx, "[KAFKA-TEST] Event loop context done", log.Data{"eventLoopContextErr": eventLoopContext.Err()})
				return
			case consumedMessage := <-chUpstream: // consumer will be nil if the broker could not be contacted, that's why we use the channel directly instead of consumer.Incomin()
				consumeCount++
				logData := log.Data{"consumeCount": consumeCount, "consumeMax": cfg.ConsumeMax, "messageOffset": consumedMessage.Offset()}
				log.Event(ctx, "[KAFKA-TEST] Received message", logData)

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

				log.Event(ctx, "[KAFKA-TEST] Message consumed", logData)
				if sleep > time.Duration(0) {
					time.Sleep(sleep)
					log.Event(ctx, "[KAFKA-TEST] done sleeping")
				}

				// send downstream
				producer.Output() <- consumedData

				if cfg.KafkaSync {
					if consumer != nil {
						log.Event(ctx, "[KAFKA-TEST] pre-release")
						consumer.CommitAndRelease(consumedMessage)
					}
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
				producer.Output() <- []byte(stdinLine)
				log.Event(ctx, "[KAFKA-TEST] Message output", log.Data{"messageSent": stdinLine, "messageChars": []byte(stdinLine)})
			}
		}
	}()

	// log errors from error channels, without exiting
	go func() {
		for true {
			select {
			case consumerError := <-chConsumerErr:
				log.Event(ctx, "[KAFKA-TEST] Consumer error", log.Error(consumerError))
			case producerError := <-chProducerErr:
				log.Event(ctx, "[KAFKA-TEST] Producer error", log.Error(producerError))
			}
		}
	}()

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
		if consumer != nil {
			log.Event(ctx, "[KAFKA-TEST] Stopping kafka consumer listener")
			consumer.StopListeningToConsumer(ctx)
		}
		log.Event(ctx, "[KAFKA-TEST] Stopped kafka consumer listener")
		eventLoopCancel()
		// wait for eventLoopDone: all in-flight messages have been processed
		<-eventLoopDone
		log.Event(ctx, "[KAFKA-TEST] Closing kafka producer")
		producer.Close(ctx)
		log.Event(ctx, "[KAFKA-TEST] Closed kafka producer")
		if consumer != nil {
			log.Event(ctx, "[KAFKA-TEST] Closing kafka consumer")
			consumer.Close(ctx)
		}
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
