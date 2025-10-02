package main

import (
	"log"
	"os"

	"github.com/Dzaakk/messaging-switcher/internal/broker"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Broker string `yaml:"broker"`
	Kafka  struct {
		Brokers []string `yaml:"brokers"`
	} `yaml:"kafka"`
	RabbitMQ struct {
		URL string `yaml:"url"`
	} `yaml:"rabbitmq"`
}

func loadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, err
	}

	return &c, nil
}

func main() {
	cfgPath := "config.yaml"
	if p := os.Getenv("CONFIG_PATH"); p != "" {
		cfgPath = p
	}

	cfg, err := loadConfig(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	var br broker.Broker
	switch cfg.Broker {
	case "rabbitmq":
		br, err = broker.NewRabbitBroker(cfg.RabbitMQ.URL)
	default:
		log.Fatalf("unkown broker: %s", cfg.Broker)
	}

	if err != nil {
		log.Fatalf("init broker: %v", err)
	}
	defer br.Close()

}
