package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/0xPCDefenders/LIBRA/models"
	"github.com/0xPCDefenders/LIBRA/utils"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Assuming you have corresponding functions for each broker type
func processETRADE(docID primitive.ObjectID, doc bson.M) error {
	// Implement ETRADE-specific processing logic here
	utils.ProduceDocument(docID, doc, "alert_etrade")
	return nil
}

func processSCHWAB(docID primitive.ObjectID, doc bson.M) error {
	// Implement SCHWAB-specific processing logic here
	utils.ProduceDocument(docID, doc, "alert_schwab")
	return nil
}

func processROBINHOOD(docID primitive.ObjectID, doc bson.M) error {
	// Implement ROBINHOOD-specific processing logic here
	// call a Kafka producer to send a message to the Kafka topic alert_robinhood
	utils.ProduceDocument(docID, doc, "alert_robinhood")
	return nil
}

func processMANUAL(docID primitive.ObjectID, doc bson.M) error {
	// Implement MANUAL-specific processing logic here
	utils.ProduceDocument(docID, doc, "alert_manual")
	return nil
}

// processMessage represents the actual normalization work.
// For now, it just prints the message but returns an error if needed.
func processMessage(msg kafka.Message) error {
	// Log the incoming message
	fmt.Printf("Processing message: topic=%s, partition=%d, offset=%d, key=%s, value=%s\n",
		msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

	// Parse the message value as JSON into the Portfolio struct
	var m models.Portfolio
	if err := json.Unmarshal(msg.Value, &m); err != nil {
		return fmt.Errorf("failed to unmarshal message value: %v", err)
	}

	// Validate required fields
	if m.Policy.UserName == "" || m.Policy.UserPass == "" {
		return fmt.Errorf("missing required fields in message")
	}

	// Convert m to bson.M
	docBytes, err := bson.Marshal(m)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}
	var doc bson.M
	if err := bson.Unmarshal(docBytes, &doc); err != nil {
		return fmt.Errorf("failed to unmarshal message to bson.M: %v", err)
	}

	// Instead of extracting brokerType from doc (as a string), use the parsed value in the struct.
	brokerType := m.Policy.BrokerType.String()
	switch brokerType {
	case "ETRADE":
		return processETRADE(m.ID, doc)
	case "SCHWAB":
		return processSCHWAB(m.ID, doc)
	case "ROBINHOOD":
		return processROBINHOOD(m.ID, doc)
	case "MANUAL":
		return processMANUAL(m.ID, doc)
	default:
		return fmt.Errorf("unknown broker type: %s", brokerType)
	}
}

// worker continuously reads messages from the buffer channel and processes them.
func worker(id int, buffer <-chan kafka.Message) {
	for msg := range buffer {
		// Wrap processing in a retry loop with panic recovery.
		var retryCount int
		for {
			func() {
				// Recover from a panic within processing.
				defer func() {
					if r := recover(); r != nil {
						log.Printf("Worker %d panicked processing message key=%s: %v", id, string(msg.Key), r)
					}
				}()
				err := processMessage(msg)
				if err != nil {
					// Processing error: log it.
					log.Printf("Worker %d: error processing message key=%s: %v", id, string(msg.Key), err)
					// Signal an error by panicking so we can retry in the loop.
					panic(err)
				}
			}()
			// If no panic occurred, break out of the retry loop.
			retryCount++
			break
		}
		// If a message fails repeatedly, you might decide to log it and drop it after some retries.
		if retryCount > 3 {
			log.Printf("Worker %d: Dropping message key=%s after %d retries", id, string(msg.Key), retryCount)
		}
	}
}

func main() {
	// Create a buffered channel for Kafka messages.
	buffer := make(chan kafka.Message, 100)

	// Start the consumer in a goroutine (this uses your updated reader code in utils).
	go utils.ConsumeToBuffer(buffer, "alert_normalize", "data-normalization-group", "../../.env")

	// Set up a ticker to print the current buffer size every 5 seconds.
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	go func() {
		for range ticker.C {
			log.Printf("Current buffer length: %d", len(buffer))
		}
	}()

	// Create a pool of workers.
	numWorkers := 5
	for i := 1; i <= numWorkers; i++ {
		go worker(i, buffer)
	}

	// Block main forever so that workers keep processing.
	select {}
}
