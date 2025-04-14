package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/0xPCDefenders/LIBRA/utils"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
)

type LLMMessage struct {
	Topic          string      `json:"topic"`
	Segment        string      `json:"segment"`
	Ticker         string      `json:"ticker"`
	Data           interface{} `json:"data"`
	Timestamp      int64       `json:"timestamp"`
	PromptTemplate string      `json:"prompt_template"`
}

type OpenAIRequest struct {
	Model       string        `json:"model"`
	Messages    []ChatMessage `json:"messages"`
	Temperature float64       `json:"temperature"`
}

type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type OpenAIResponse struct {
	Choices []struct {
		Message ChatMessage `json:"message"`
	} `json:"choices"`
}

type DeepSeekRequest struct {
	Model    string        `json:"model"`
	Messages []ChatMessage `json:"messages"`
	Stream   bool          `json:"stream"`
}

type Allocation struct {
	Ticker          string  `json:"ticker"`
	OptimizedWeight float64 `json:"optimized_weight"`
	LLMAnalysis     string  `json:"llm_analysis"`
	ValueDifference float64 `json:"value_difference"`
}

type Portfolio struct {
	ID          string    `json:"_id"`
	UserID      string    `json:"userId"`
	Name        string    `json:"name"`
	Holdings    []Holding `json:"holdings"`
	Policy      Policy    `json:"policy"`
	LastUpdated string    `json:"lastUpdated"`
}

type Holding struct {
	Symbol       string  `json:"symbol"`
	Name         string  `json:"name"`
	Quantity     int     `json:"quantity"`
	CostBasis    float64 `json:"costBasis"`
	CurrentPrice float64 `json:"currentPrice"`
	Currency     string  `json:"currency"`
}

type Policy struct {
	UserID             string           `json:"userId"`
	RiskTolerance      float64          `json:"riskTolerance"`
	InvestmentHorizon  int              `json:"investmentHorizon"`
	BrokerType         int              `json:"brokerType"`
	TargetAllocation   TargetAllocation `json:"targetAllocation"`
	RebalanceFrequency string           `json:"rebalanceFrequency"`
}

type TargetAllocation struct {
	UserID      string      `json:"userId"`
	Name        string      `json:"name"`
	Allocations interface{} `json:"allocations"`
	LastUpdated string      `json:"lastUpdated"`
}

func callDeepSeekAPI(prompt string) (string, error) {
	apiKey := os.Getenv("DEEPSEEK_API_KEY")
	if apiKey == "" {
		return "", fmt.Errorf("DEEPSEEK_API_KEY not set in environment")
	}

	url := "https://api.deepseek.com/chat/completions"

	request := DeepSeekRequest{
		Model: "deepseek-chat",
		Messages: []ChatMessage{
			{
				Role:    "system",
				Content: "Look at the following portfolio holding data for the user and provide a detailed analysis of the portfolio's performance, key metrics, and potential risks or opportunities with the ticker symbol in relation to fundamentals and sentiment.",
			},
			{
				Role:    "user",
				Content: prompt,
			},
		},
		Stream: false,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("error marshaling request: %v", err)
	}

	// Implement retry logic
	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
		if err != nil {
			return "", fmt.Errorf("error creating request: %v", err)
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+apiKey)

		// Increased timeout to 90 seconds
		client := &http.Client{Timeout: 90 * time.Second}

		// Add context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		req = req.WithContext(ctx)

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Attempt %d failed: %v", attempt, err)
			if attempt == maxRetries {
				return "", fmt.Errorf("max retries reached: %v", err)
			}
			time.Sleep(time.Second * time.Duration(attempt)) // Exponential backoff
			continue
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Attempt %d failed reading body: %v", attempt, err)
			if attempt == maxRetries {
				return "", fmt.Errorf("max retries reached: %v", err)
			}
			time.Sleep(time.Second * time.Duration(attempt))
			continue
		}

		if resp.StatusCode != http.StatusOK {
			log.Printf("Attempt %d failed with status %d: %s", attempt, resp.StatusCode, string(body))
			if attempt == maxRetries {
				return "", fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
			}
			time.Sleep(time.Second * time.Duration(attempt))
			continue
		}

		var response OpenAIResponse
		if err := json.Unmarshal(body, &response); err != nil {
			return "", fmt.Errorf("error unmarshaling response: %v", err)
		}

		if len(response.Choices) == 0 {
			return "", fmt.Errorf("no response choices received")
		}

		return response.Choices[0].Message.Content, nil
	}

	return "", fmt.Errorf("failed after %d retries", maxRetries)
}

func processLLMMessage(job interface{}) interface{} {
	message := job.(string)

	// Add debug logging
	log.Printf("Received data: %v", message)

	// Parse portfolio data directly from the JSON string
	var portfolio Portfolio
	if err := json.Unmarshal([]byte(message), &portfolio); err != nil {
		log.Printf("Error unmarshaling portfolio data: %v", err)
		return nil
	}

	// Create allocations slice
	var allocations []Allocation

	// Process each holding
	for _, holding := range portfolio.Holdings {
		// Prepare prompt for DeepSeek
		prompt := fmt.Sprintf("Analyze the following financial data and provide detailed insights about the company's financial health, key metrics, and potential risks or opportunities. %s", holding.Symbol, holding)

		// Call DeepSeek API
		analysis, err := callDeepSeekAPI(prompt)
		if err != nil {
			log.Printf("Error calling DeepSeek API for %s: %v", holding.Symbol, err)
			continue
		}

		// Create allocation object
		allocation := Allocation{
			Ticker:          holding.Symbol,
			OptimizedWeight: holding.CurrentPrice,
			LLMAnalysis:     analysis,
			ValueDifference: 0, // Calculate based on your requirements
		}
		allocations = append(allocations, allocation)
	}

	// Get MongoDB client from utils (you'll need to implement this)
	client := utils.GetMongoClient()
	if client == nil {
		log.Printf("Failed to get MongoDB client")
		return nil
	}

	// Update MongoDB document
	collection := client.Database("Integrations").Collection("Portfolios")
	filter := bson.M{"userId": portfolio.UserID} // Assuming segment is the user ID
	update := bson.M{
		"$set": bson.M{
			"policy.target_allocations": allocations,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		log.Printf("Error updating MongoDB document: %v", err)
		return nil
	}

	if result.MatchedCount == 0 {
		log.Printf("No document found for user %s", portfolio.UserID)
		return nil
	}

	log.Printf("Successfully updated allocations for user %s", portfolio.UserID)
	return allocations
}

func StartLLMProcessor() error {
	// Create worker pool with 5 workers
	workerPool := utils.NewWorkerPool(5, 100, processLLMMessage)
	workerPool.Start()

	// Listen for messages from alert_llm topic
	alertBuffer := make(chan kafka.Message)
	go utils.ConsumeToBuffer(alertBuffer, "alert_optimize", "llm-optimization-group", ".env")

	// Process messages from the buffer
	for msg := range alertBuffer {
		workerPool.Submit(string(msg.Value))
	}

	return nil
}

func main() {
	// Load environment variables
	if err := godotenv.Load(".env"); err != nil {
		log.Printf("Warning: Error loading .env file: %v", err)
	}

	log.Println("Starting Portfolio Optimizer...")
	if err := StartLLMProcessor(); err != nil {
		log.Fatalf("Error starting Portfolio Optimizer: %v", err)
	}
}
