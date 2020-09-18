package main

import (
	"bufio"
	"context"
	"errors"
	"os"
	"os/signal"
	"time"

	kafka "github.com/ONSdigital/dp-kafka"
	"github.com/ONSdigital/log.go/log"
	"github.com/kelseyhightower/envconfig"
)

const serviceName = "kafka-example-producer"

// Config is the kafka configuration for this example
type Config struct {
	Brokers                 []string      `envconfig:"KAFKA_ADDR"`
	KafkaMaxBytes           int           `envconfig:"KAFKA_MAX_BYTES"`
	KafkaVersion            string        `envconfig:"KAFKA_VERSION"`
	ProducedTopic           string        `envconfig:"KAFKA_PRODUCED_TOPIC"`
	GracefulShutdownTimeout time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	Chomp                   bool          `envconfig:"CHOMP_MSG"`
}

// period of time between tickers
const ticker = 3 * time.Second

func main() {
	log.Namespace = serviceName
	ctx := context.Background()

	if err := run(ctx); err != nil {
		log.Event(ctx, "fatal runtime error", log.Error(err), log.FATAL)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill)

	// Read Config
	cfg := &Config{
		Brokers:                 []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		KafkaMaxBytes:           50 * 1024 * 1024,
		KafkaVersion:            "2.3.1",
		ProducedTopic:           "myTopic",
		GracefulShutdownTimeout: 5 * time.Second,
		Chomp:                   false,
	}
	if err := envconfig.Process("", cfg); err != nil {
		return err
	}

	// run kafka Producer
	producer, err := runProducer(ctx, cfg)
	if err != nil {
		return err
	}

	// blocks until an os interrupt or a fatal error occurs
	sig := <-signals
	log.Event(ctx, "os signal received", log.Data{"signal": sig}, log.INFO)
	return closeProducer(ctx, producer, cfg.GracefulShutdownTimeout)
}

func runProducer(ctx context.Context, cfg *Config) (*kafka.Producer, error) {
	stdinChannel := make(chan string)

	log.Event(ctx, "[KAFKA-TEST] Starting Producer (stdin sent to producer)", log.INFO, log.Data{"config": cfg})
	kafka.SetMaxMessageSize(int32(cfg.KafkaMaxBytes))

	// Create Producer with channels
	pChannels := kafka.CreateProducerChannels()
	producer, err := kafka.NewProducer(
		ctx, cfg.Brokers, cfg.ProducedTopic, cfg.KafkaMaxBytes, pChannels)
	if err != nil {
		return nil, err
	}

	// If it is not initialised at creation time, create a go-routine to retry
	if !producer.IsInitialised() {
		log.Event(ctx, "[KAFKA-TEST] Producer could not be initialised at creation time. Please, try to initialise it later.", log.WARN)
		initialiserLoop(ctx, producer)
	}

	// go-routine to log errors from error channel
	pChannels.LogErrors(ctx, "[KAFKA-TEST] Producer error")

	// Create loop-control channel and context
	eventLoopContext, eventLoopCancel := context.WithCancel(ctx)
	eventLoopDone := make(chan bool)

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
				log.Event(ctx, "[KAFKA-TEST] tick", log.INFO)

			case <-eventLoopContext.Done():
				log.Event(ctx, "[KAFKA-TEST] Event loop context done", log.INFO, log.Data{"eventLoopContextErr": eventLoopContext.Err()})
				return

			case stdinLine := <-stdinChannel:
				// Used for this example to write messages to kafka consumer topic (should not be needed in applications)
				pChannels.Output <- []byte(stdinLine)
				log.Event(ctx, "[KAFKA-TEST] Message output", log.INFO, log.Data{"messageSent": stdinLine, "messageChars": []byte(stdinLine)})
			}
		}
	}()
	return producer, nil
}

func closeProducer(ctx context.Context, producer *kafka.Producer, gracefulShutdownTimeout time.Duration) error {
	log.Event(ctx, "commencing graceful shutdown", log.Data{"graceful_shutdown_timeout": gracefulShutdownTimeout}, log.INFO)
	ctx, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)

	// track shutown gracefully closes up
	var hasShutdownError bool

	// background graceful shutdown
	go func() {
		defer cancel()
		log.Event(ctx, "[KAFKA-TEST] Closing kafka producer", log.INFO)
		if err := producer.Close(ctx); err != nil {
			hasShutdownError = true
		}
		log.Event(ctx, "[KAFKA-TEST] Closed kafka producer", log.INFO)
	}()

	// wait for timeout or success (via cancel)
	<-ctx.Done()

	if ctx.Err() == context.DeadlineExceeded {
		log.Event(ctx, "[KAFKA-TEST] graceful shutdown timed out", log.WARN, log.Error(ctx.Err()))
		return ctx.Err()
	}

	if hasShutdownError {
		err := errors.New("failed to shutdown gracefully")
		log.Event(ctx, "failed to shutdown gracefully ", log.ERROR, log.Error(err))
		return err
	}

	log.Event(ctx, "graceful shutdown was successful", log.INFO)
	return nil
}

// initialiserLoop retries to initialise a producer until it is initialised or the closer channel is closed
// TODO - we might want to move this loop to kafka Producer itself.
func initialiserLoop(ctx context.Context, producer *kafka.Producer) {
	go func() {
		for {
			select {
			case <-producer.Channels().Closer:
				log.Event(ctx, "[KAFKA-TEST] Producer has not been initialised, but it is being closed. Aborting initialisation loop", log.INFO)
				return
			default:
				if err := producer.Initialise(ctx); err != nil {
					time.Sleep(ticker)
					continue
				}
				log.Event(ctx, "[KAFKA-TEST] Producer has been successfully initialised", log.INFO)
				return
			}
		}
	}()
}
