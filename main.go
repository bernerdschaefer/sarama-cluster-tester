package main

import (
	"context"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/heroku/cedar/lib/kafka"
	"github.com/joeshaw/envdecode"
	"github.com/pborman/uuid"
	"golang.org/x/sync/errgroup"
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
	case "stream":
		if err := stream(cfg.Topic, addrs, config); err != nil {
			log.Fatal(err)
		}
	case "consume":
		if err := consume(os.Args[2], cfg.Topic, addrs, config); err != nil {
			log.Fatal(err)
		}
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

func stream(topic string, addrs []string, cfg *cluster.Config) error {
	c, err := sarama.NewClient(addrs, &cfg.Config)
	if err != nil {
		return err
	}
	defer c.Close()

	cg, err := kafka.NewConsumerGroup(c, topic,
		kafka.WithUpdatingOffsets(),
		kafka.WithStreamToCurrent(),
	)
	if err != nil {
		return err
	}
	defer cg.Close()

	for {
		msg, err := cg.GetMessage(context.TODO())
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		dump(msg)
	}

	return nil
}

func consume(group string, topic string, addrs []string, cfg *cluster.Config) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	start := make(chan struct{})
	results := make(chan string)

	g.Go(func() error {
		return doConsumeBad(ctx, start, group, topic, addrs, cfg, results)
	})

	for i := 0; i < 4; i++ {
		g.Go(func() error {
			return doConsume(ctx, start, group, topic, addrs, cfg, results)
		})
	}

	g.Go(func() error {
		defer cancel()
		found := map[string]struct{}{}
		seen := 0

		for {
			select {
			case i := <-results:
				seen++
				found[i] = struct{}{}

				log.Printf("at=result found=%d seen=%d", len(found), seen)

				if len(found) == 1000 {
					return nil
				}
			case <-ctx.Done():
				return nil
			}
		}
	})

	log.Printf("at=wait")
	<-time.After(15 * time.Second)
	close(start)

	return g.Wait()
}

// doConsumeBad consumes 10 messages without acknowledging them and then closes
// the consumer.
func doConsumeBad(ctx context.Context, start chan struct{}, group string, topic string, addrs []string, cfg *cluster.Config, results chan string) error {
	cg, err := cluster.NewConsumer(addrs, group, []string{topic}, cfg)
	if err != nil {
		return err
	}
	defer cg.Close()

	<-start

	seen := 0

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-cg.Messages():
			seen++

			dump(msg)

			select {
			case results <- string(msg.Value):
			case <-ctx.Done():
				return nil
			}

			if seen >= 10 {
				return nil
			}
		case err := <-cg.Errors():
			return err
		case not := <-cg.Notifications():
			log.Printf(
				"at=rebalance claimed=%+v released=%+v current=%+v",
				not.Claimed,
				not.Released,
				not.Current,
			)
		}
	}

	return nil
}

func doConsume(ctx context.Context, start chan struct{}, group string, topic string, addrs []string, cfg *cluster.Config, results chan string) error {
	cg, err := cluster.NewConsumer(addrs, group, []string{topic}, cfg)
	if err != nil {
		return err
	}
	defer cg.Close()

	<-start

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-cg.Messages():
			dump(msg)

			select {
			case results <- string(msg.Value):
			case <-ctx.Done():
				return nil
			}

			cg.MarkOffset(msg, "done")
		case err := <-cg.Errors():
			return err
		case not := <-cg.Notifications():
			log.Printf(
				"at=rebalance claimed=%+v released=%+v current=%+v",
				not.Claimed,
				not.Released,
				not.Current,
			)
		}
	}

	return nil
}

func dump(msg *sarama.ConsumerMessage) {
	log.Printf(
		"at=msg topic=%s partition=%d offset=%d key=%s value=%s",
		msg.Topic,
		msg.Partition,
		msg.Offset,
		msg.Key,
		msg.Value,
	)
}
