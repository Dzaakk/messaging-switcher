package broker

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbitBroker struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	url  string
}

func NewRabbitBroker(url string) (Broker, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	if err := ch.Confirm(false); err != nil {
		log.Printf("warning: rabbitmq channel confirm not enabled: %v", err)
	}
	return &rabbitBroker{conn: conn, ch: ch, url: url}, nil
}

func (r *rabbitBroker) Publish(topic string, key []byte, value []byte, headers map[string]string) error {
	ex := "demo.exchange"
	if err := r.ch.ExchangeDeclare(ex, "direct", true, false, false, false, nil); err != nil {
		return err
	}

	amqHeaders := amqp.Table{}
	for k, v := range headers {
		amqHeaders[k] = v
	}
	pk := amqp.Publishing{
		ContentType: "application/octet-stream",
		Body:        value,
		Timestamp:   time.Now(),
		Headers:     amqHeaders,
	}

	if err := r.ch.Publish(ex, topic, false, false, pk); err != nil {
		return err
	}
	log.Printf("[rabbitmq] published to exchange=%s routing=%s", ex, topic)
	return nil
}

// Consume implements Broker.
func (r *rabbitBroker) Consume(ctx context.Context, topic string, group string, handler Handler) error {
	panic("unimplemented")
}

// Close implements Broker.
func (r *rabbitBroker) Close() error {
	panic("unimplemented")
}
