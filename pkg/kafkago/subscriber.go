package kafkago

import (
	"context"
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
	closingCh    chan struct{}
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
		closingCh:    make(chan struct{}),
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

	consumeMessageClosed, err := s.consumeMessages(ctx, topic, outputCh, logFields)
	if err != nil {
		s.logger.Error("consume messages failed", err, logFields)
		s.subscriberWg.Done()
		return nil, err
	}
	go func() {
		<-consumeMessageClosed
		close(outputCh)
		s.subscriberWg.Done()
	}()

	return outputCh, nil
}

func (s *Subscriber) consumeMessages(
	ctx context.Context,
	topic string,
	output chan *message.Message,
	logFields watermill.LogFields,
) (consumeMessageClosed chan struct{}, err error) {
	s.logger.Info("Starting consuming", logFields)

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-s.closingCh:
			s.logger.Debug("Closing subscriber, cancelling consumeMessages", logFields)
			cancel()
		case <-ctx.Done():

		}
	}()

	consumeMessageClosed, err = s.readMessages(ctx, topic, output, logFields)
	if err != nil {
		s.logger.Debug(
			"Starting consume failed, cancelling context",
			logFields.Add(watermill.LogFields{"err": err}),
		)
		cancel()
		return nil, err
	}

	return consumeMessageClosed, nil
}

func (s *Subscriber) readMessages(ctx context.Context, topic string, outputCh chan *message.Message, logFields watermill.LogFields) (chan struct{}, error) {
	messageHandler := s.newMessageHandler(outputCh)
	consumeMessageClosed := make(chan struct{})

	go func() {
		defer close(consumeMessageClosed)
		for {
			select {
			case <-s.closingCh:
				s.logger.Debug("Subscriber is closing, stopping read message", logFields)
				return
			case <-ctx.Done():
				s.logger.Debug("Ctx was cancelled, stopping read message", logFields)
				return
			default:
				kmsg, err := s.reader.FetchMessage(ctx)
				if err != nil {
					s.logger.Error("fetch message fail", err, logFields)
					if err == context.Canceled {
						return
					}
					continue
				}
				if err := messageHandler.process(ctx, topic, &kmsg, logFields); err != nil {
					s.logger.Error("process message fail", err, logFields)
					return
				}
			}
		}

	}()

	return consumeMessageClosed, nil
}

//func (s *Subscriber) processMessage(ctx context.Context, kmsg *kafka.Message, outputCh chan<- *message.Message, logFields watermill.LogFields) error {
//	s.logger.Trace(fmt.Sprintf("process message:%s", string(kmsg.Value)), logFields)
//	msg, err := s.config.Unmarshaler.Unmarshal(kmsg)
//	if err != nil {
//		return errors.Wrap(err, "message unmarshal failed")
//	}
//	sctx, cancelFn := context.WithCancel(ctx)
//	msg.SetContext(sctx)
//	defer cancelFn()
//
//	logFields = logFields.Add(watermill.LogFields{
//		"message_uuid": msg.UUID,
//	})
//
//ResendLoop:
//	for {
//		select {
//		case outputCh <- msg:
//			s.logger.Trace("Message sent to consumer", logFields)
//		case <-s.closingCh:
//			s.logger.Trace("Closing, message discarded", logFields)
//			return nil
//		case <-sctx.Done():
//			s.logger.Trace("Closing, ctx cannceled before sent to consumer", logFields)
//			return nil
//		}
//
//		select {
//		case <-msg.Acked():
//			s.logger.Trace("Message Acked", logFields)
//
//			if err := s.reader.CommitMessages(ctx, *kmsg); err != nil {
//				s.logger.Error("Commit message failed", err, logFields)
//			}
//			break ResendLoop
//		case <-msg.Nacked():
//			s.logger.Trace("Message Nacked", logFields)
//			msg = msg.Copy()
//			if s.config.NackResendSleep != NoSleep {
//				time.Sleep(s.config.NackResendSleep)
//			}
//
//			continue ResendLoop
//		case <-s.closingCh:
//			s.logger.Trace("Closing, message discarded before ack", logFields)
//			return nil
//		case <-ctx.Done():
//			s.logger.Trace("Closing, ctx canceled before ack", logFields)
//			return nil
//		}
//	}
//	return nil
//}

func (s *Subscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true
	close(s.closingCh)
	s.subscriberWg.Wait()

	if s.reader != nil {
		if err := s.reader.Close(); err != nil {
			s.logger.Error("Cannot close kafka reader", err, watermill.LogFields{})
		}
	}

	return nil
}

func (s *Subscriber) newMessageHandler(outputCh chan *message.Message) messageHandler {
	return messageHandler{
		outputCh:        outputCh,
		reader:          s.reader,
		consumerGroup:   s.config.ConsumerGroup,
		unmarshaler:     s.config.Unmarshaler,
		nackResendSleep: s.config.NackResendSleep,
		logger:          s.logger,
		closingCh:       s.closingCh,
	}
}

type messageHandler struct {
	outputCh        chan<- *message.Message
	reader          *kafka.Reader
	consumerGroup   string
	unmarshaler     Unmarshaler
	nackResendSleep time.Duration
	logger          watermill.LoggerAdapter
	closingCh       chan struct{}
}

func (h *messageHandler) process(ctx context.Context, topic string, kmsg *kafka.Message, logFields watermill.LogFields) error {

	msgLogFields := logFields
	//msgLogFields := logFields.Add(watermill.LogFields{
	//	"xid": msg.,
	//})

	h.logger.Trace("Received message from kafka", msgLogFields)

	msg, err := h.unmarshaler.Unmarshal(kmsg)
	if err != nil {
		return errors.Wrapf(err, "message unmarshal failed")
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	msg.SetContext(ctx)
	defer cancelCtx()

	msgLogFields = msgLogFields.Add(watermill.LogFields{
		"message_uuid": msg.UUID,
	})

ResendLoop:
	for {
		select {
		case h.outputCh <- msg:
			h.logger.Trace("Message sent to consumer", msgLogFields)
		case <-h.closingCh:
			h.logger.Trace("Closing, message discard", msgLogFields)
			return nil
		case <-ctx.Done():
			h.logger.Trace("Closing, ctx cancelled before sent to consumer", msgLogFields)
			return nil
		}

		select {
		case <-msg.Acked():
			if err := h.reader.CommitMessages(ctx, *kmsg); err != nil {
				h.logger.Error("Message acked failed", err, msgLogFields)
			} else {
				h.logger.Trace("Message acked", logFields)
			}
			break ResendLoop
		case <-msg.Nacked():
			h.logger.Trace("Message nacked", msgLogFields)
			msg = msg.Copy()
			if h.nackResendSleep != NoSleep {
				time.Sleep(h.nackResendSleep)
			}

			continue ResendLoop
		case <-h.closingCh:
			h.logger.Trace("Closing, message discard before ack", msgLogFields)
			return nil
		case <-ctx.Done():
			h.logger.Trace("Closing, ctx cancelled before ack", msgLogFields)
			return nil
		}
	}

	return nil
}
