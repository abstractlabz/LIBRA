package utils

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

func ExampleWriter() {
	// SASL credentials: these come from your Confluent Cloud cluster.
	saslMechanism := plain.Mechanism{
		Username: "CL6JWUSO4GMZZ4RF",
		Password: "cxRWjlLx00pHKpt5Rs0BRMHYTmbq3zyMTHG3sPl5CAQJQsHvtnzz1kb/tsNH9d/I",
	}

	// Create a TLS config. In many cases, a basic config is enough.
	// If you're doing mutual TLS or custom certs, you'll need more setup.
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// Create a dialer that uses SASL/PLAIN over TLS.
	dialer := &kafka.Dialer{
		SASLMechanism: saslMechanism,
		TLS:           tlsConfig,
	}

	// Connect specifically to the leader for partition 0 of the topic.
	conn, err := dialer.DialLeader(
		context.Background(),
		"tcp",
		"pkc-921jm.us-east-2.aws.confluent.cloud:9092",
		"alert_normalize",
		0,
	)
	if err != nil {
		log.Printf("Error connecting to Kafka: %v", err)
		return
	}
	defer conn.Close() // important to close!

	// Write a test message
	_, err = conn.WriteMessages(
		kafka.Message{
			Key:   []byte("hello"),
			Value: []byte("world"),
		},
	)
	if err != nil {
		log.Printf("Error writing message to Kafka: %v", err)
		return
	}

	fmt.Println("Message written successfully to Kafka.")
}
