package main

import (
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/garyburd/redigo/redis"
	"github.com/pkg/errors"
)

func consumeConfluent(brokers []string, pool *redis.Pool) error {
	conn := pool.Get()
	defer conn.Close()
	prefix := os.Getenv("KEY_PREFIX")

	const (
		caCert     = "/tmp/kafka-ca-cert"
		clientCert = "/tmp/kafka-client-cert"
		clientKey  = "/tmp/kafka-client-key"
	)

	if err := ioutil.WriteFile(caCert, []byte(os.Getenv("KAFKA_TRUSTED_CERT")), os.ModePerm); err != nil {
		return err
	}

	if err := ioutil.WriteFile(clientCert, []byte(os.Getenv("KAFKA_CLIENT_CERT")), os.ModePerm); err != nil {
		return err
	}

	if err := ioutil.WriteFile(clientKey, []byte(os.Getenv("KAFKA_CLIENT_CERT_KEY")), os.ModePerm); err != nil {
		return err
	}

	cfg := &kafka.ConfigMap{
		"metadata.broker.list": strings.Join(brokers, ","),
		"group.id":             os.Getenv("GROUP_ID"),

		// Manually commit messages to ensure rebalancing doesn't skip events.
		"enable.auto.commit": false,

		// If there is no existing group to join, start at the beginning of the
		// topic instead of the end.
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "earliest"},

		// Opt into explicit rebalancing so we're notified about changes.
		"go.application.rebalance.enable": true,

		// The event channel is noted to be faster than the Poll-based API.
		"go.events.channel.enable": true,
		"go.events.channel.size":   1000, // default

		// SSL setup
		"security.protocol":        "ssl",
		"ssl.ca.location":          caCert,
		"ssl.certificate.location": clientCert,
		"ssl.key.location":         clientKey,
	}

	consumer, err := kafka.NewConsumer(cfg)
	if err != nil {
		return errors.Wrap(err, "consumer setup")
	}
	defer consumer.Close()

	if err := consumer.Subscribe(os.Getenv("TOPIC"), nil); err != nil {
		return errors.Wrap(err, "subscribe to topic")
	}

	tick := time.Tick(time.Second)
	total := 0

	for {
		select {
		case <-tick:
			log.Printf("at=tick count#consumer-total=%d", total)
			total = 0

		case ev := <-consumer.Events():
			total++

			switch e := ev.(type) {
			case *kafka.Message:
				v, err := strconv.Atoi(string(e.Value))
				if err != nil {
					return errors.Wrap(err, "invalid message value")
				}

				key := prefix + ":" + string(e.Key)
				_, err = conn.Do("SETBIT", key, v, 1)
				if err != nil {
					return errors.Wrap(err, "set redis bit")
				}

				consumer.CommitMessage(e)

			case kafka.AssignedPartitions:
				log.Printf("at=assigned-partitions partitions=%+v", e.Partitions)
				if err := consumer.Assign(e.Partitions); err != nil {
					return errors.Wrap(err, "assign partitions")
				}

			case kafka.RevokedPartitions:
				log.Printf("at=revoked-partitions partitions=%+v", e.Partitions)
				if err := consumer.Unassign(); err != nil {
					return errors.Wrap(err, "unassign partitions")
				}

			case kafka.PartitionEOF:
				// The consumer has reached the end of the partition. This is purely
				// informational -- the consumer will keep trying to fetch messages for
				// this partition.
				log.Printf("at=partition-eof")

			case kafka.OffsetsCommitted:
				log.Printf("at=committed offsets=%+v", e.Error, e.Offsets)

				if e.Error != nil {
					return errors.Wrap(e.Error, "commit offset error")
				}

			case kafka.Error:
				return errors.Wrap(e, "unknown error")
			}
		}
	}
}
