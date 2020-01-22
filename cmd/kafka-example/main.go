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
const ticker = 1 * time.Second

// Number of ticks between health checks
const healthTickerPeriod = 5

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
		log.Event(ctx, "[KAFKA-TEST] Could not create producer. Please, try to reconnect later", log.Error(err))
	}

	// Create Consumer with channels
	cgChannels := kafka.CreateConsumerGroupChannels(cfg.KafkaSync)
	consumer, err := kafka.NewConsumerWithChannels(
		ctx, cfg.Brokers, cfg.ConsumedTopic, cfg.ConsumedGroup, kafka.OffsetNewest, cfg.KafkaSync, cgChannels)
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
		tick := 0
		for {
			select {
			case <-time.After(ticker):
				tick++
				log.Event(ctx, "[KAFKA-TEST] tick")
				if tick >= healthTickerPeriod {
					tick = 0
					performHealthchecks(&consumer, &producer)
				}
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
				producer.Output() <- []byte(stdinLine)
				log.Event(ctx, "[KAFKA-TEST] Message output", log.Data{"messageSent": stdinLine, "messageChars": []byte(stdinLine)})
			}
		}
	}()

	// log errors from consumer error channel
	go func() {
		for true {
			select {
			case consumerError := <-cgChannels.Errors:
				log.Event(ctx, "[KAFKA-TEST] Consumer error", log.Error(consumerError))
			case <-cgChannels.Closer:
				return
			}
		}
	}()

	// log errors from producer error channel
	go func() {
		for true {
			select {
			case producerError := <-pChannels.Errors:
				log.Event(ctx, "[KAFKA-TEST] Producer error", log.Error(producerError))
			case <-pChannels.Closer:
				return
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

// performHealthchecks triggers healthchecks in consumer and producer, and logs the result.
func performHealthchecks(consumer *kafka.ConsumerGroup, producer *kafka.Producer) {
	ctx := context.Background()
	pCheck, pErr := producer.Checker(ctx)
	if pErr != nil {
		log.Event(ctx, "[KAFKA-TEST] Producer healthcheck error", log.Error(pErr))
	}
	log.Event(ctx, "[KAFKA-TEST] Producer healthcheck", log.Data{"check": pCheck})
	cCheck, cErr := consumer.Checker(ctx)
	if cErr != nil {
		log.Event(ctx, "[KAFKA-TEST] Consumer healthcheck error", log.Error(cErr))
	}
	log.Event(ctx, "[KAFKA-TEST] Consumer healthcheck", log.Data{"check": cCheck})
}
