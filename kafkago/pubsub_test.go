package kafkago

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/stretchr/testify/require"
	"testing"
)

func kafkaBrokers() []string {
	//return []string{"localhost:9091", "localhost:9092", "localhost:9093", "localhost:9094", "localhost:9095"}
	return []string{"localhost:9091"}
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
