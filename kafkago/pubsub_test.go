package kafkago

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func kafkaBrokers() []string {
	return []string{"kafka1:9091", "kafka2:9092", "kafka3:9093", "kafka4:9094", "kafka5:9095"}
	//return []string{"localhost:29092"}
}

func newPubSub(t *testing.T, marshaler MarshalerUnmarshaler, consumerGroup string) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(true, true)

	var err error
	var publisher message.Publisher

	publisher = NewPublisher(PublisherConfig{
		Brokers:     kafkaBrokers(),
		Async:       false,
		Marshaler:   marshaler,
		OTELEnabled: false,
		Ipv4Only:    true,
		Timeout:     100 * time.Second,
	}, logger)

	var subscriber message.Subscriber
	subscriber, err = NewSubscriber(SubscriberConfig{
		Brokers:       kafkaBrokers(),
		Unmarshaler:   marshaler,
		ConsumerGroup: consumerGroup,
		OTELEnabled:   false,
	}, logger)
	require.NoError(t, err)
	return publisher, subscriber
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return newPubSub(t, DefaultMarshaler{}, "test")
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(t, DefaultMarshaler{}, consumerGroup)
}

func TestPublisherSubscriber(t *testing.T) {
	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: false,
		GuaranteedOrder:     false,
		Persistent:          true,
	}
	tests.TestPubSub(t, features, createPubSub, createPubSubWithConsumerGroup)
}

func TestPublish(t *testing.T) {
	pub, _ := newPubSub(t, DefaultMarshaler{}, "test")
	require.NotNil(t, pub)
	err := pub.Publish("test", message.NewMessage(watermill.NewULID(), message.Payload("helloworld")))
	require.NoError(t, err)
}
