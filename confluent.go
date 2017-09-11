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
	commitTick := time.Tick(5 * time.Second)
	total := 0
	partitions := []kafka.TopicPartition{}

	for {
		select {
		case <-tick:
			log.Printf("at=tick count#consumer-total=%d", total)
			total = 0

		case <-commitTick:
			start := time.Now()
			_, err := consumer.CommitOffsets(partitions)
			log.Printf("at=commit measure#commit-time=%s", time.Since(start))
			if err != nil {
				return errors.Wrap(err, "committing offsets")
			}

		case ev := <-consumer.Events():
			total++

			switch e := ev.(type) {
			case *kafka.Message:
				if partitions == nil {
					log.Printf("at=no-partition") // message processed after we were unassigned
					continue
				}

				v, err := strconv.Atoi(string(e.Value))
				if err != nil {
					return errors.Wrap(err, "invalid message value")
				}

				key := prefix + ":" + string(e.Key)
				_, err = conn.Do("SETBIT", key, v, 1)
				if err != nil {
					return errors.Wrap(err, "set redis bit")
				}

				partitions[int(e.TopicPartition.Partition)] = e.TopicPartition

			case kafka.AssignedPartitions:
				partitions = e.Partitions

				log.Printf("at=assigned-partitions partitions=%+v", e.Partitions)
				if err := consumer.Assign(e.Partitions); err != nil {
					return errors.Wrap(err, "assign partitions")
				}

			case kafka.RevokedPartitions:
				partitions = nil

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
