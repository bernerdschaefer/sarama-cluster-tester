package main

import (
	"crypto/tls"
	"crypto/x509"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/heroku/cedar/lib/kafka"
)

// AddrsConfig takes Config and generates the broker addresses and
// sarama.Config.
func AddrsConfig(cfg Config) ([]string, *cluster.Config, error) {
	addrs, err := kafka.Addrs(cfg.URL)
	if err != nil {
		return nil, nil, err
	}

	config := cluster.NewConfig()
	config.Version = sarama.V0_10_2_0

	config.Consumer.Offsets.Initial = cfg.ConsumerOffsetsInitial
	config.Consumer.Return.Errors = true

	config.Group.Return.Notifications = true

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true

	if err := config.Validate(); err != nil {
		return nil, nil, err
	}

	if err := configureTLS(config, cfg.Cert, cfg.Key, cfg.CACert); err != nil {
		return nil, nil, err
	}

	return addrs, config, nil
}

func configureTLS(cfg *cluster.Config, cert, key, caCert string) error {
	if cert == "" || key == "" || caCert == "" {
		return nil
	}

	tlsConfig, err := newTLSConfig(cert, key, caCert)
	if err != nil {
		return err
	}

	cfg.Net.TLS.Config = tlsConfig
	cfg.Net.TLS.Enable = true

	return nil
}

func newTLSConfig(clientCert, clientKey, caCert string) (*tls.Config, error) {
	cert, err := tls.X509KeyPair([]byte(clientCert), []byte(clientKey))
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM([]byte(caCert))
	config := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
		RootCAs:            caCertPool,
	}
	config.BuildNameToCertificate()
	return config, nil
}
