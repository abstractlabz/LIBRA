package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/0xPCDefenders/LIBRA/utils"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Update your struct to handle MongoDB extended JSON format
type BrokerType struct {
	NumberInt string `json:"$numberInt,omitempty"`
}

// Then in your main struct that contains broker type
type Policy struct {
	BrokerType        BrokerType `json:"brokerType"`
	InvestmentHorizon struct {
		NumberInt string `json:"$numberInt,omitempty"`
	} `json:"investmentHorizon"`
	// ... other fields ...
}

type NumberInt struct {
	Value string `json:"$numberInt"`
}

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

func processCOINBASE(docID primitive.ObjectID, doc bson.M) error {
	// Implement COINBASE-specific processing logic here
	utils.ProduceDocument(docID, doc, "alert_coinbase")
	return nil
}

// processMessage represents the actual normalization work.
// For now, it just prints the message but returns an error if needed.
func processMessage(msg kafka.Message) error {
	// Log the incoming message
	fmt.Printf("Processing message: topic=%s, partition=%d, offset=%d, key=%s, value=%s\n",
		msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

	// First step: unmarshal directly into a map to handle MongoDB extended JSON properly
	var rawMap map[string]interface{}
	if err := json.Unmarshal(msg.Value, &rawMap); err != nil {
		return fmt.Errorf("failed to unmarshal message value: %v", err)
	}

	// Convert rawMap to bson.M
	docBytes, err := bson.Marshal(rawMap)
	if err != nil {
		return fmt.Errorf("failed to marshal message to BSON: %v", err)
	}

	var doc bson.M
	if err := bson.Unmarshal(docBytes, &doc); err != nil {
		return fmt.Errorf("failed to unmarshal to bson.M: %v", err)
	}

	// Extract brokerType from the policy
	policy, ok := doc["policy"].(bson.M)
	if !ok {
		return fmt.Errorf("policy field not found or invalid format")
	}

	// Here's the key change: MongoDB extended JSON should come through as a proper BSON type now
	var brokerType string
	var brokerTypeInt int

	// Handle different possible formats of brokerType in the document
	switch bt := policy["brokerType"].(type) {
	case int32:
		brokerTypeInt = int(bt)
	case int64:
		brokerTypeInt = int(bt)
	case float64:
		brokerTypeInt = int(bt)
	case bson.M:
		// Handle extended JSON format
		if numStr, ok := bt["$numberInt"].(string); ok {
			var convErr error
			brokerTypeInt, convErr = strconv.Atoi(numStr)
			if convErr != nil {
				return fmt.Errorf("invalid broker type number: %v", convErr)
			}
		} else {
			return fmt.Errorf("invalid broker type format: %v", bt)
		}
	default:
		return fmt.Errorf("unknown broker type format: %T %v", policy["brokerType"], policy["brokerType"])
	}

	// Map numeric values to broker type strings
	switch brokerTypeInt {
	case 1:
		brokerType = "ETRADE"
	case 2:
		brokerType = "COINBASE"
	case 3:
		brokerType = "SCHWAB"
	case 4:
		brokerType = "MANUAL"
	default:
		return fmt.Errorf("unknown broker type code: %d", brokerTypeInt)
	}

	// Extract ID from the document
	idHex, ok := rawMap["_id"].(string)
	if !ok {
		return fmt.Errorf("_id field not found or invalid format")
	}

	objectID, err := primitive.ObjectIDFromHex(idHex)
	if err != nil {
		return fmt.Errorf("invalid _id format: %v", err)
	}

	// Use the extracted broker type for processing
	switch brokerType {
	case "ETRADE":
		return processETRADE(objectID, doc)
	case "SCHWAB":
		return processSCHWAB(objectID, doc)
	case "COINBASE":
		return processCOINBASE(objectID, doc)
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
	go utils.ConsumeToBuffer(buffer, "alert_normalize", "data-normalization-group", ".env")

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
