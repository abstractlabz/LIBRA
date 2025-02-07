package main

import (
	"fmt"
	"log"
	"time"

	"github.com/0xPCDefenders/LIBRA/utils"
	"github.com/segmentio/kafka-go"
)

// processMessage represents the actual normalization work.
// For now, it just prints the message but returns an error if needed.
func processMessage(msg kafka.Message) error {
	// Simulate processing: replace with actual normalization logic.
	fmt.Printf("Processing message: topic=%s, partition=%d, offset=%d, key=%s, value=%s\n",
		msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
	// TODO: Add actual normalization logic here.
	//Convert the value from msg.Value into a json and
	return nil
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
			// If we reached here without a panic, assume processing was successful.
			// (In a more complex system, you might check a flag.)
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
	go utils.ConsumeToBuffer(buffer)

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
