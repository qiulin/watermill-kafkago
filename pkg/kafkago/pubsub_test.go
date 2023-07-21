package kafkago

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func kafkaBrokers() []string {
	//return []string{"kafka1:9091", "kafka2:9092", "kafka3:9093", "kafka4:9094", "kafka5:9095"}
	return []string{"kafka1:9091"}
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
	pub, sub := newPubSub(t, DefaultMarshaler{}, "test")
	require.NotNil(t, pub)
	wg := sync.WaitGroup{}
	wg.Add(1)
	ch, err := sub.Subscribe(context.Background(), "test")
	require.NoError(t, err)
	go func() {
		for {
			select {
			case msg := <-ch:
				fmt.Println(string(msg.Payload))
				wg.Done()
			}
		}
	}()
	err = pub.Publish("test", message.NewMessage(watermill.NewULID(), message.Payload("helloworld")))
	require.NoError(t, err)
	wg.Wait()
	fmt.Println("ok")
}
