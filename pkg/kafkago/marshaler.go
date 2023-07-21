package kafkago

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"time"
)

const UUIDHeaderKey = "_watermill_message_uuid"
const HeaderKey = "_key"

type Marshaler interface {
	Marshal(topic string, msg *message.Message) (*kafka.Message, error)
}

type Unmarshaler interface {
	Unmarshal(*kafka.Message) (*message.Message, error)
}

type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}

type DefaultMarshaler struct {
}

func (d DefaultMarshaler) Marshal(topic string, msg *message.Message) (*kafka.Message, error) {

	if value := msg.Metadata.Get(UUIDHeaderKey); value != "" {
		return nil, errors.Errorf("metadata %s is reserved by watermill for message UUID", UUIDHeaderKey)
	}

	headers := []kafka.Header{
		{
			Key:   UUIDHeaderKey,
			Value: []byte(msg.UUID),
		},
	}

	var key string
	for k, v := range msg.Metadata {
		if k == HeaderKey {
			key = v
		} else {
			headers = append(headers, kafka.Header{
				Key:   k,
				Value: []byte(v),
			})
		}
	}

	return &kafka.Message{
		Topic:   topic,
		Key:     []byte(key),
		Value:   msg.Payload,
		Headers: headers,
		Time:    time.Now(),
	}, nil
}

func (DefaultMarshaler) Unmarshal(kmsg *kafka.Message) (*message.Message, error) {

	var msgID string
	metadata := make(message.Metadata, len(kmsg.Headers))
	for _, header := range kmsg.Headers {
		if header.Key == UUIDHeaderKey {
			msgID = string(header.Value)
		} else {
			metadata.Set(header.Key, string(header.Value))
		}
	}

	msg := message.NewMessage(msgID, kmsg.Value)
	msg.Metadata = metadata
	return msg, nil
}
