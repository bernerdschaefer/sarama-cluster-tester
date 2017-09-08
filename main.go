package main

import (
	"log"
	"os"
	"strconv"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/joeshaw/envdecode"
	"github.com/pborman/uuid"
)

// Config is all the necessary kafka bits to bootstrap a client, consumer,
// or producer.
type Config struct {
	CACert string `env:"KAFKA_TRUSTED_CERT"`
	Cert   string `env:"KAFKA_CLIENT_CERT"`
	Key    string `env:"KAFKA_CLIENT_CERT_KEY"`
	URL    string `env:"KAFKA_URL,required"`

	Topic   string `env:"TOPIC"`
	GroupID string `env:"GROUP_ID"`

	ConsumerOffsetsInitial int64 `env:"KAFKA_CONSUMER_OFFSET_INITIAL,default=-2"`
}

func main() {
	var cfg Config
	envdecode.MustStrictDecode(&cfg)

	addrs, config, err := AddrsConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}

	switch os.Args[1] {
	case "produce":
		if err := produce(cfg.Topic, addrs, config); err != nil {
			log.Fatal(err)
		}
	case "consume":
	default:
		println("Usage: consume | produce")
		os.Exit(1)
	}
}

func produce(topic string, addrs []string, cfg *cluster.Config) error {
	p, err := sarama.NewSyncProducer(addrs, &cfg.Config)
	if err != nil {
		return err
	}
	defer p.Close()

	for i := 0; i < 1000; i++ {
		id := uuid.New()

		_, _, err := p.SendMessage(&sarama.ProducerMessage{
			Key:   sarama.StringEncoder(id),
			Value: sarama.StringEncoder(strconv.Itoa(i)),
			Topic: topic,
		})

		if err != nil {
			return err
		}
	}

	return nil
}
