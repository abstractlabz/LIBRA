package main

import (
	"encoding/json"
	"log"

	"github.com/0xPCDefenders/LIBRA/models"
	"github.com/0xPCDefenders/LIBRA/utils"
	"github.com/segmentio/kafka-go"
)

// emptyProcess is a stub function that gets called each time
// valid portfolio data is received from the alert_rebalance topic.
func emptyProcess(portfolio models.Portfolio) {
	log.Printf("Empty process: received portfolio for user %s with name %s and %d holdings",
		portfolio.UserID, portfolio.Name, len(portfolio.Holdings))
	// TODO: Add your processing logic here.
}

// consumeAlertRebalance creates a buffered channel and launches the reader from utils,
// then processes messages from the "alert_rebalance" topic concurrently.
func consumeAlertRebalance() {
	// Create a buffered channel for Kafka messages.
	buffer := make(chan kafka.Message, 100)

	// Start the consumer from utils/reader.go to read messages from "alert_rebalance".
	go utils.ConsumeToBuffer(buffer, "alert_rebalance", "rebalancer-group", "../.env")

	log.Println("Rebalancer is listening to the alert_rebalance topic...")

	// Continuously process messages from the buffer.
	for msg := range buffer {
		// Process each message concurrently.
		go func(m kafka.Message) {
			var p models.Portfolio
			if err := json.Unmarshal(m.Value, &p); err != nil {
				log.Printf("Failed to unmarshal portfolio: %v", err)
				return
			}
			emptyProcess(p)
		}(msg)
	}
}

func main() {
	log.Println("Starting rebalancer...")
	consumeAlertRebalance()
}
