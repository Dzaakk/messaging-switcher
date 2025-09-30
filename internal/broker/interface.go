package broker

import "context"

type Message struct {
	Key     []byte
	Value   []byte
	Headers map[string]string

	Ack  func() error
	Nack func() error
}

type Handler func(msg Message) error

type Broker interface {
	Publish(topic string, key []byte, value []byte, headers map[string]string) error
	Consume(ctx context.Context, topic string, group string, handler Handler) error
	Close() error
}
