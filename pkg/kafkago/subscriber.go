package kafkago

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"sync"
	"time"
)

const NoSleep time.Duration = -1

type SubscriberConfig struct {
	Brokers []string

	Unmarshaler Unmarshaler

	OverrideReaderConfig kafka.ReaderConfig

	ConsumerGroup string

	NackResendSleep time.Duration

	ReconnectRetrySleep time.Duration

	OTELEnabled bool
}

func (c SubscriberConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("missing brokers")
	}
	if c.Unmarshaler == nil {
		return errors.New("missing unmarshaler")
	}

	return nil
}

type Subscriber struct {
	config       SubscriberConfig
	logger       watermill.LoggerAdapter
	reader       *kafka.Reader
	closing      chan struct{}
	subscriberWg sync.WaitGroup
	closed       bool
}

func NewSubscriber(config SubscriberConfig, logger watermill.LoggerAdapter) (message.Subscriber, error) {

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	logger = logger.With(watermill.LogFields{
		"subscriber_uuid": watermill.NewShortUUID(),
	})

	return &Subscriber{
		config:       config,
		logger:       logger,
		closing:      make(chan struct{}),
		subscriberWg: sync.WaitGroup{},
		closed:       false,
	}, nil
}

func newReader(config SubscriberConfig, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:               config.Brokers,
		GroupID:               config.ConsumerGroup,
		WatchPartitionChanges: true,
		Topic:                 topic,
	})
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber closed")
	}

	s.reader = newReader(s.config, topic)

	s.subscriberWg.Add(1)
	logFields := watermill.LogFields{
		"provider":            "kafkago",
		"topic":               topic,
		"consumer_group":      s.config.ConsumerGroup,
		"kafka_consumer_uuid": watermill.NewShortUUID(),
	}

	s.logger.Info("Subscribing to Kafka topic", logFields)

	outputCh := make(chan *message.Message)

	go func() {
		err := s.consumeMessages(ctx, outputCh, logFields)
		if err != nil {
			s.logger.Error("consume messages failed", err, logFields)
			s.subscriberWg.Done()
			//return nil, err
		}
	}()

	return outputCh, nil
}

func (s *Subscriber) consumeMessages(
	ctx context.Context,
	output chan *message.Message,
	logFields watermill.LogFields,
) (err error) {
	s.logger.Info("Starting consuming", logFields)

	for {
		select {
		case <-s.closing:
			s.logger.Debug("Subscriber is closing, stopping consume", logFields)
			return nil
		case <-ctx.Done():
			s.logger.Debug("Ctx was canceled, stopping consume", logFields)
			return nil
		default:
			kmsg, err := s.reader.FetchMessage(ctx)
			if err != nil {
				if err == context.Canceled {
					return nil
				}
				s.logger.Error("fetch messages from kafka error", err, logFields)
				continue
			}

			if err := s.processMessage(ctx, &kmsg, output, logFields); err != nil {
				s.logger.Error("process messages error", err, logFields)
				return nil
			}

		}
	}
}

func (s *Subscriber) processMessage(ctx context.Context, kmsg *kafka.Message, outputCh chan<- *message.Message, logFields watermill.LogFields) error {
	s.logger.Trace(fmt.Sprintf("process message:%s", string(kmsg.Value)), logFields)
	msg, err := s.config.Unmarshaler.Unmarshal(kmsg)
	if err != nil {
		return errors.Wrap(err, "message unmarshal failed")
	}
	sctx, cancelFn := context.WithCancel(ctx)
	msg.SetContext(sctx)
	defer cancelFn()

	logFields = logFields.Add(watermill.LogFields{
		"message_uuid": msg.UUID,
	})

ResendLoop:
	for {
		select {
		case outputCh <- msg:
			s.logger.Trace("Message sent to consumer", logFields)
		case <-s.closing:
			s.logger.Trace("Closing, message discarded", logFields)
			return nil
		case <-sctx.Done():
			s.logger.Trace("Closing, ctx cannceled before sent to consumer", logFields)
			return nil
		}

		select {
		case <-msg.Acked():
			s.logger.Trace("Message Acked", logFields)

			if err := s.reader.CommitMessages(ctx, *kmsg); err != nil {
				s.logger.Error("Commit message failed", err, logFields)
			}
			break ResendLoop
		case <-msg.Nacked():
			s.logger.Trace("Message Nacked", logFields)
			msg = msg.Copy()
			if s.config.NackResendSleep != NoSleep {
				time.Sleep(s.config.NackResendSleep)
			}

			continue ResendLoop
		case <-s.closing:
			s.logger.Trace("Closing, message discarded before ack", logFields)
			return nil
		case <-ctx.Done():
			s.logger.Trace("Closing, ctx canceled before ack", logFields)
			return nil
		}
	}
	return nil
}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}
	if s.reader != nil {
		if err := s.reader.Close(); err != nil {
			s.logger.Error("Cannot close kafka reader", err, watermill.LogFields{})
		}
	}
	s.closed = true
	close(s.closing)
	s.subscriberWg.Wait()

	return nil
}
