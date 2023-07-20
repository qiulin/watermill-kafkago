package kafkago

import (
	"context"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"net"
	"strconv"
)

type PublisherConfig struct {
	Writer *kafka.Writer

	Brokers []string

	Async bool

	// Marshaler is used to marshal messages from Watermill format into Kafka format.
	Marshaler Marshaler

	// If true then each sent message will be wrapped with Opentelemetry tracing, provided by otelsarama.
	OTELEnabled bool
}

func (c *PublisherConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("missing brokers")
	}
	if c.Marshaler == nil {
		return errors.New("missing marshaler")
	}

	return nil
}

func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) message.Publisher {
	var writer = config.Writer
	if writer == nil {
		writer = newWriter(config)
	}
	return &Publisher{
		config: config,
		writer: writer,
		logger: logger,
	}
}

func newWriter(config PublisherConfig) *kafka.Writer {

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(config.Brokers...),
		Balancer:               &kafka.LeastBytes{},
		Async:                  config.Async,
		AllowAutoTopicCreation: true,
	}

	return writer
}

type Publisher struct {
	config PublisherConfig
	writer *kafka.Writer
	logger watermill.LoggerAdapter

	closed bool
}

func (p *Publisher) createTopic(topic string) error {
	addr := p.config.Brokers[0]
	logFields := watermill.LogFields{
		"addr":  addr,
		"topic": topic,
	}
	p.logger.Trace("Creating kafka topic", logFields)
	conn, err := kafka.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctrl, err := conn.Controller()
	if err != nil {
		return err
	}
	logFields.Add(watermill.LogFields{"ctrl_host": ctrl.Host, "ctrl_port": ctrl.Port})
	p.logger.Trace("Dial kafka control conn", logFields)
	ctrlConn, err := kafka.Dial("tcp", net.JoinHostPort(ctrl.Host, strconv.Itoa(ctrl.Port)))
	if err != nil {
		return err
	}
	defer ctrlConn.Close()
	err = ctrlConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil {
		return err
	}
	p.logger.Trace("Created kafka topic", logFields)
	return nil
}

func (p *Publisher) Publish(topic string, msgs ...*message.Message) error {
	if p.closed {
		return errors.New("publisher closed")
	}

	logFields := make(watermill.LogFields, 4)
	logFields["topic"] = topic

	//if err := p.createTopic(topic); err != nil {
	//	p.logger.Error("Auto create topic error", err, logFields)
	//	return err
	//}

	for _, msg := range msgs {
		logFields["message_uuid"] = msg.UUID
		p.logger.Trace("Sending message to Kafka", logFields)
		kmsg, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
		}
		ctx := context.Background()
		if err := p.writer.WriteMessages(ctx, *kmsg); err != nil {
			return errors.Wrapf(err, "cannot produce message %s", msg.UUID)
		}

		p.logger.Trace("Message sent to Kafka", logFields)
	}

	return nil
}

func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true

	if err := p.writer.Close(); err != nil {
		return errors.Wrap(err, "cannot close Kafka writer")
	}
	return nil
}
