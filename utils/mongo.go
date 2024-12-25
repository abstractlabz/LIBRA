// utils/mongo.go
package utils

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ConnectToMongo establishes a connection to MongoDB using the URI from the environment variable.
func ConnectToMongo() (*mongo.Client, error) {
	// Load the .env file. Adjust the path if your .env is located elsewhere.
	err := godotenv.Load("../.env")
	if err != nil {
		log.Println("Error loading .env file:", err)
		return nil, fmt.Errorf("failed to load .env file: %v", err)
	}

	// Retrieve the MongoDB URI from the environment variable.
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		log.Println("MONGO_URI is not set in the environment variables")
		return nil, fmt.Errorf("MONGO_URI environment variable is not set")
	}

	// Configure MongoDB client options.
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	clientOptions := options.Client().
		ApplyURI(mongoURI).
		SetServerAPIOptions(serverAPI)

	// Connect to MongoDB.
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Println("Couldn't connect to database:", err)
		return nil, fmt.Errorf("couldn't connect to database: %v", err)
	}

	// Ping the database to verify the connection.
	if err := client.Ping(context.TODO(), nil); err != nil {
		log.Println("Couldn't ping database:", err)
		return nil, fmt.Errorf("couldn't ping database: %v", err)
	}

	log.Println("Connected to MongoDB successfully")
	return client, nil
}
