package broker

import (
	"context"
	"fmt"
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

func (r *rabbitBroker) Consume(ctx context.Context, topic string, group string, handler Handler) error {
	ex := "demo.exchange"
	if err := r.ch.ExchangeDeclare(ex, "direct", true, false, false, false, nil); err != nil {
		return err
	}

	qName := fmt.Sprintf("%s.%s", topic, group)
	q, err := r.ch.QueueDeclare(qName, true, false, false, false, nil)
	if err != nil {
		return err
	}
	if err := r.ch.QueueBind(q.Name, topic, ex, false, nil); err != nil {
		return err
	}

	msgs, err := r.ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case d, ok := <-msgs:
				if !ok {
					log.Println("[rabbitmq] deliveries channel closed")
					return
				}
				hdrs := map[string]string{}
				for k, v := range d.Headers {
					if s, ok := v.(string); ok {
						hdrs[k] = s
					}
				}
				m := Message{
					Key:     nil,
					Value:   d.Body,
					Headers: hdrs,
					Ack: func() error {
						return d.Ack(false)
					},
					Nack: func() error {
						return d.Nack(false, true)
					},
				}
				if err := handler(m); err != nil {
					log.Printf("[rabbitmq] handler err: %v", err)

					if err := m.Nack(); err != nil {
						log.Printf("nack err: %v", err)
					}
				} else {
					if err := m.Ack(); err != nil {
						log.Printf("ack err: %v", err)
					}
				}
			case <-ctx.Done():
				log.Println("[rabbitmq] consumer context done")
				return
			}
		}
	}()
	return nil
}

func (r *rabbitBroker) Close() error {
	if r.ch != nil {
		_ = r.ch.Close()
	}
	if r.conn != nil {
		_ = r.conn.Close()
	}
	return nil
}
