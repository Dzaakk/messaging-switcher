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
	config := sarama.NewConfig()
	config.Version = sarama.V2_5_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	client, err := sarama.NewConsumerGroup(k.brokers, group, config)
	if err != nil {
		return err
	}

	k.group = client
	consumer := &kafkaConsumerGroupHandler{handler: handler}

	go func() {
		defer client.Close()
		for {
			if err := client.Consume(ctx, []string{topic}, consumer); err != nil {
				log.Printf("[kafka] consume error: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	return nil
}

func (k *kafkaBroker) Close() error {
	if k.producers != nil {
		_ = k.producers.Close()
	}
	if k.group != nil {
		_ = k.group.Close()
	}
	return nil
}

type kafkaConsumerGroupHandler struct {
	handler Handler
}

// Cleanup implements sarama.ConsumerGroupHandler.
func (k *kafkaConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	panic("unimplemented")
}

// ConsumeClaim implements sarama.ConsumerGroupHandler.
func (k *kafkaConsumerGroupHandler) ConsumeClaim(sarama.ConsumerGroupSession, sarama.ConsumerGroupClaim) error {
	panic("unimplemented")
}

// Setup implements sarama.ConsumerGroupHandler.
func (k *kafkaConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	panic("unimplemented")
}
