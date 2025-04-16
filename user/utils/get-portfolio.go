package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/fernet/fernet-go"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Global MongoDB client instance for re-use among handlers
var mongoClient *mongo.Client

// EncryptPassword encrypts a plain text password using Fernet symmetric encryption
// Returns the encrypted password as a base64-encoded string or an error if encryption fails
func EncryptPassword(password string) (string, error) {
	// Get encryption key from environment variable
	encryptionKey := os.Getenv("PASSWORD_ENCRYPTION_KEY")
	if encryptionKey == "" {
		return "", fmt.Errorf("password encryption key not found in environment variables")
	}

	// Process the key to ensure it's in the correct format for Fernet
	key, err := formatFernetKey(encryptionKey)
	if err != nil {
		return "", fmt.Errorf("failed to format encryption key: %w", err)
	}

	// Create a new Fernet key from the formatted key
	k, err := fernet.DecodeKey(key)
	if err != nil {
		return "", fmt.Errorf("failed to decode Fernet key: %w", err)
	}

	// Encrypt the password
	encryptedBytes, err := fernet.EncryptAndSign([]byte(password), k)
	if err != nil {
		return "", fmt.Errorf("failed to encrypt password: %w", err)
	}
	encryptedPassword := string(encryptedBytes)

	return encryptedPassword, nil
}

// DecryptPassword decrypts a Fernet-encrypted password
// Returns the decrypted password or an error if decryption fails
func DecryptPassword(encryptedPassword string) (string, error) {
	// Get encryption key from environment variable
	encryptionKey := os.Getenv("PASSWORD_ENCRYPTION_KEY")
	if encryptionKey == "" {
		return "", fmt.Errorf("password encryption key not found in environment variables")
	}

	// Process the key to ensure it's in the correct format for Fernet
	key, err := formatFernetKey(encryptionKey)
	if err != nil {
		return "", fmt.Errorf("failed to format encryption key: %w", err)
	}

	// Create a new Fernet key from the formatted key
	k, err := fernet.DecodeKey(key)
	if err != nil {
		return "", fmt.Errorf("failed to decode Fernet key: %w", err)
	}

	// Decrypt the password
	msg := fernet.VerifyAndDecrypt([]byte(encryptedPassword), time.Hour*24*30, []*fernet.Key{k})
	if msg == nil {
		return "", fmt.Errorf("invalid token or incorrect key")
	}

	return string(msg), nil
}

// formatFernetKey ensures the key is properly formatted for Fernet
// (32 url-safe base64-encoded bytes)
func formatFernetKey(key string) (string, error) {
	// Check if the key is already properly formatted
	if len(key) == 44 && key[len(key)-1] == '=' {
		// Key is likely already in the correct format
		return key, nil
	}

	// Convert the key to bytes
	keyBytes := []byte(key)

	// Pad or truncate to 32 bytes
	paddedKey := make([]byte, 32)
	copy(paddedKey, keyBytes)

	// Generate a URL-safe base64-encoded key
	encodedKey := base64.URLEncoding.EncodeToString(paddedKey)
	return encodedKey, nil
}

// GenerateFernetKey generates a new random Fernet key and returns it as a string
func GenerateFernetKey() (string, error) {
	key := fernet.Key{}
	err := key.Generate()
	if err != nil {
		return "", fmt.Errorf("failed to generate Fernet key: %w", err)
	}
	return key.Encode(), nil
}

func main() {
	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var err error
	// Connect to MongoDB

	mongoURI, err := godotenv.Read(".env")
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}
	mongoClient, err = mongo.Connect(ctx, options.Client().ApplyURI(mongoURI["MONGO_URI"]))
	if err != nil {
		log.Fatalf("Error connecting to MongoDB: %v", err)
	}

	// Ping the database to verify connectivity
	if err := mongoClient.Ping(ctx, nil); err != nil {
		log.Fatalf("Error pinging MongoDB: %v", err)
	}
	log.Println("Connected to MongoDB successfully")

	// Initialize the Gin router
	router := gin.Default()

	// Define the endpoint that retrieves the portfolio
	router.GET("/getPortfolio", getPortfolioHandler)

	// Create HTTPS server with SSL
	server := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	// Load SSL certificates
	certFile := "server.crt"
	keyFile := "server.key"

	// Check if certificates exist, if not generate them
	if _, err := os.Stat(certFile); os.IsNotExist(err) {
		log.Println("SSL certificates not found. Generating self-signed certificates...")
		cmd := exec.Command("openssl", "req", "-x509", "-newkey", "rsa:4096", "-nodes",
			"-out", certFile, "-keyout", keyFile,
			"-days", "365", "-subj", "/CN=portfolio.fineasapp.io")
		if err := cmd.Run(); err != nil {
			log.Fatalf("Failed to generate SSL certificates: %v", err)
		}
	}

	// Start HTTPS server
	log.Println("Starting HTTPS server on :8080")
	if err := server.ListenAndServeTLS(certFile, keyFile); err != nil {
		log.Fatalf("Failed to start HTTPS server: %v", err)
	}
}

// getPortfolioHandler handles GET requests to /getPortfolio
// It expects a query parameter "id" which contains the MongoDB ObjectID.
// The handler connects to MongoDB, selects the "Portfolios" database and "integrations" collection,
// and retrieves the document with the specified ObjectID.
func getPortfolioHandler(c *gin.Context) {
	// Retrieve the ObjectID from query parameter "id"
	idParam := c.Query("id")
	if idParam == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing id parameter"})
		return
	}

	// Convert the string ID to MongoDB ObjectID
	objectID, err := primitive.ObjectIDFromHex(idParam)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id format"})
		return
	}

	// Access the "integrations" collection within the "Portfolios" database.
	collection := mongoClient.Database("Integrations").Collection("Portfolios")

	// Set up a context with a timeout for the MongoDB operation.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create the filter to match the document with the specified ObjectID.
	filter := bson.M{"_id": objectID}

	// Define a result holder - using a generic map since the structure is unspecified.
	var portfolio bson.M
	err = collection.FindOne(ctx, filter).Decode(&portfolio)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			c.JSON(http.StatusNotFound, gin.H{"error": "portfolio not found"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "error retrieving portfolio"})
		}
		return
	}

	// Return the retrieved portfolio as JSON.
	c.JSON(http.StatusOK, portfolio)
}
