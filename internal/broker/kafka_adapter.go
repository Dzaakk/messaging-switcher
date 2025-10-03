package broker

import (
	"context"
	"log"

	"github.com/IBM/sarama"
)

type kafkaBroker struct {
	producers sarama.SyncProducer
	group     sarama.ConsumerGroup
	brokers   []string
}

func NewKafkabroker(brokers []string) (Broker, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	config.Version = sarama.V2_5_0_0

	prod, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &kafkaBroker{
		producers: prod,
		brokers:   brokers,
	}, nil
}

func (k *kafkaBroker) Publish(topic string, key []byte, value []byte, headers map[string]string) error {
	var hs []sarama.RecordHeader
	for k, v := range headers {
		hs = append(hs, sarama.RecordHeader{Key: []byte(k), Value: []byte(v)})
	}

	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Key:     sarama.ByteEncoder(key),
		Value:   sarama.ByteEncoder(value),
		Headers: hs,
	}
	partition, offset, err := k.producers.SendMessage(msg)
	if err != nil {
		return err
	}
	log.Printf("[kafka] produced to partition=%d offset=%d", partition, offset)
	return nil
}

func (k *kafkaBroker) Consume(ctx context.Context, topic string, group string, handler Handler) error {
	panic("unimplemented")
}

func (k *kafkaBroker) Close() error {
	panic("unimplemented")
}
