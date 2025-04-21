package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"crypto/tls"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Environment variables for Coinbase credentials and redirect URI.
var (
	clientID     string
	clientSecret string
	redirectURI  string
)

func init() {
	if err := godotenv.Load(".env"); err != nil {
		log.Printf("Warning: Error loading .env file: %v", err)
	}
	clientID = os.Getenv("COINBASE_OAUTH_CLIENT_ID")
	clientSecret = os.Getenv("COINBASE_OAUTH_SECRET")
	redirectURI = os.Getenv("COINBASE_REDIRECT_URI")
}

// Coinbase OAuth and API endpoints.
const (
	coinbaseAuthURL     = "https://www.coinbase.com/oauth/authorize"
	coinbaseTokenURL    = "https://api.coinbase.com/oauth/token"
	coinbaseAccountsURL = "https://api.coinbase.com/v2/accounts"
)

// Session struct to hold tokens.
type Session struct {
	AccessToken  string
	RefreshToken string
	UserParams   map[string]interface{}
}

// A simple in-memory session store with a mutex for thread safety.
var (
	sessionStore = make(map[string]Session)
	sessionMutex sync.Mutex
)

// generateSessionID creates a random session ID.
func generateSessionID() string {
	return fmt.Sprintf("%d", rand.Int63())
}

// getSession retrieves or creates a session for the incoming request.
// It uses a cookie named "session_id" to track the session.
func getSession(w http.ResponseWriter, r *http.Request) (string, Session) {
	cookie, err := r.Cookie("session_id")
	if err != nil || cookie.Value == "" {
		// No session exists; create a new one.
		sessionID := generateSessionID()
		cookie = &http.Cookie{
			Name:     "session_id",
			Value:    sessionID,
			Path:     "/",
			HttpOnly: true,
		}
		http.SetCookie(w, cookie)
		sessionMutex.Lock()
		sessionStore[sessionID] = Session{}
		sessionMutex.Unlock()
		return sessionID, sessionStore[sessionID]
	}

	sessionMutex.Lock()
	sess, exists := sessionStore[cookie.Value]
	if !exists {
		sess = Session{}
		sessionStore[cookie.Value] = sess
	}
	sessionMutex.Unlock()
	return cookie.Value, sess
}

// CORS middleware to handle cross-origin requests
func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Max-Age", "3600")

		// Handle preflight requests
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Call the next handler
		next(w, r)
	}
}

// indexHandler redirects the user to Coinbase's OAuth authorization URL.
func indexHandler(w http.ResponseWriter, r *http.Request) {
	// Get user parameters from URL query
	requestBody := r.URL.Query().Get("requestBody")
	var userParams map[string]interface{}
	if requestBody != "" {
		decodedBody, err := url.QueryUnescape(requestBody)
		if err != nil {
			log.Printf("Error decoding request body: %v", err)
		} else {
			if err := json.Unmarshal([]byte(decodedBody), &userParams); err != nil {
				log.Printf("Error parsing user parameters: %v", err)
			}
		}
	}

	// Store user parameters in session
	sessionID, sess := getSession(w, r)
	sessionMutex.Lock()
	sess.UserParams = userParams
	sessionStore[sessionID] = sess
	sessionMutex.Unlock()

	scope := "wallet:accounts:read"
	// Build the authorization URL with query parameters.
	authURL, err := url.Parse(coinbaseAuthURL)
	if err != nil {
		http.Error(w, "Failed to parse Coinbase auth URL", http.StatusInternalServerError)
		return
	}
	params := url.Values{}
	params.Add("client_id", clientID)
	params.Add("redirect_uri", "https://base.fineasapp.io:2083/callback") // Use explicit callback path
	params.Add("response_type", "code")
	params.Add("scope", scope)
	authURL.RawQuery = params.Encode()

	http.Redirect(w, r, authURL.String(), http.StatusFound)
}

// callbackHandler handles Coinbase's redirection with the authorization code,
// then exchanges the code for an access token.
func callbackHandler(w http.ResponseWriter, r *http.Request) {
	code := r.URL.Query().Get("code")
	if code == "" {
		http.Error(w, "No code provided in callback", http.StatusBadRequest)
		return
	}

	// Get session and user parameters
	sessionID, sess := getSession(w, r)
	userID := getUserParamString(sess.UserParams, "userId", "")
	if userID == "" {
		http.Error(w, "No user ID found in session parameters", http.StatusBadRequest)
		return
	}

	// Prepare the POST form data to exchange the code for a token.
	data := url.Values{}
	data.Set("grant_type", "authorization_code")
	data.Set("code", code)
	data.Set("client_id", clientID)
	data.Set("client_secret", clientSecret)
	data.Set("redirect_uri", redirectURI)

	resp, err := http.PostForm(coinbaseTokenURL, data)
	if err != nil {
		http.Error(w, "Error making request to token endpoint: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, "Error reading token response: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Parse the JSON response.
	var tokenResp struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		http.Error(w, "Error parsing token response: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if tokenResp.AccessToken == "" {
		http.Error(w, "No access token found in response", http.StatusInternalServerError)
		return
	}

	// Save the tokens in the session
	sessionMutex.Lock()
	sess.AccessToken = tokenResp.AccessToken
	sess.RefreshToken = tokenResp.RefreshToken
	sessionStore[sessionID] = sess
	sessionMutex.Unlock()

	// Store tokens in MongoDB using the actual user ID instead of session ID
	err = storeTokens(userID, tokenResp.AccessToken, tokenResp.RefreshToken, tokenResp.ExpiresIn)
	if err != nil {
		log.Printf("Warning: Failed to store tokens in MongoDB: %v", err)
	}

	// Get holdings data
	client := &http.Client{}
	req, err := http.NewRequest("GET", coinbaseAccountsURL, nil)
	if err != nil {
		http.Error(w, "Error creating request: "+err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("Authorization", "Bearer "+tokenResp.AccessToken)

	holdingsResp, err := client.Do(req)
	if err != nil {
		http.Error(w, "Error fetching account holdings: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer holdingsResp.Body.Close()

	holdingsBody, err := ioutil.ReadAll(holdingsResp.Body)
	if err != nil {
		http.Error(w, "Error reading holdings response: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Convert holdings to portfolio
	portfolio, err := convertCoinbaseHoldingsToPortfolio(holdingsBody, sess.UserParams)
	if err != nil {
		http.Error(w, "Error converting holdings: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Upload to MongoDB
	portfolioID, err := uploadPortfolioToMongo(portfolio)
	if err != nil {
		log.Printf("Warning: Failed to upload to MongoDB: %v", err)
	}

	// Produce to Kafka
	if err := produceToKafka(portfolio); err != nil {
		log.Printf("Warning: Failed to produce to Kafka: %v", err)
	}

	// Return success HTML page
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprintf(w, `
	<!DOCTYPE html>
	<html>
	<head>
		<title>Coinbase Connection Successful</title>
		<style>
			body {
				font-family: Arial, sans-serif;
				display: flex;
				justify-content: center;
				align-items: center;
				height: 100vh;
				margin: 0;
				background-color: #0a0b1e;
				color: #fff;
			}
			.container {
				text-align: center;
				padding: 2rem;
				background-color: #151633;
				border-radius: 12px;
				box-shadow: 0 4px 6px rgba(0,0,0,0.3);
				max-width: 400px;
			}
			.success-icon {
				width: 48px;
				height: 48px;
				margin: 0 auto 1rem auto;
			}
			.success-icon svg {
				width: 100%;
				height: 100%;
			}
			.success-icon svg path {
				fill: #a855f7;
			}
			h1 {
				color: #fff;
				margin-bottom: 1rem;
				font-weight: 500;
			}
			p {
				color: #a9a9c7;
				margin-bottom: 2rem;
				line-height: 1.5;
			}
			button {
				background-color: #a855f7;
				color: white;
				border: none;
				padding: 12px 24px;
				border-radius: 8px;
				cursor: pointer;
				font-size: 16px;
				transition: all 0.2s ease;
			}
			button:hover {
				background-color: #9333ea;
				transform: translateY(-1px);
			}
		</style>
	</head>
	<body>
		<div class="container">
			<div class="success-icon">
				<svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
					<path d="M20.285 2l-11.285 11.567-5.286-5.011-3.714 3.716 9 8.728 15-15.285z"/>
				</svg>
			</div>
			<h1>Connection Successful!</h1>
			<p>Your Coinbase account has been successfully connected. You can now close this window and return to the application.</p>
			<button onclick="window.close()">Close Window</button>
		</div>
		<script>
			// Notify parent window of successful connection
			if (window.opener) {
				window.opener.postMessage({ type: 'COINBASE_CONNECTION_SUCCESS', portfolioId: '`+portfolioID.Hex()+`' }, '*');
			}
		</script>
	</body>
	</html>
	`)
}

// Portfolio struct and related types (copied from models package)
type Portfolio struct {
	ID              primitive.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	UserID          string             `bson:"userId" json:"userId"`
	Name            string             `bson:"name" json:"name"`
	Holdings        []Holding          `bson:"holdings" json:"holdings"`
	TotalValue      float64            `bson:"totalValue" json:"totalValue"`
	TotalInvestment float64            `bson:"totalInvestment" json:"totalInvestment"`
	StartDate       time.Time          `bson:"startDate" json:"startDate"`
	EndDate         time.Time          `bson:"endDate" json:"endDate"`
	Policy          UserPolicy         `bson:"policy" json:"policy"`
	Performance     PerformanceMetrics `bson:"performance" json:"performance"`
	LastUpdated     time.Time          `bson:"lastUpdated" json:"lastUpdated"`
}

type Holding struct {
	Symbol           string  `bson:"symbol" json:"symbol"`
	Name             string  `bson:"name" json:"name"`
	Quantity         float64 `bson:"quantity" json:"quantity"`
	CostBasis        float64 `bson:"costBasis" json:"costBasis"`
	CurrentPrice     float64 `bson:"currentPrice" json:"currentPrice"`
	Currency         string  `bson:"currency" json:"currency"`
	Category         string  `bson:"category" json:"category"`
	Beta             float64 `bson:"beta" json:"beta"`
	StartMarketValue float64 `bson:"startMarketValue" json:"startMarketValue"`
	EndMarketValue   float64 `bson:"endMarketValue" json:"endMarketValue"`
	RebalancedShares float64 `bson:"rebalancedShares" json:"rebalancedShares"`
	RebalanceCash    float64 `bson:"rebalanceCash" json:"rebalanceCash"`
	ValueDifference  float64 `bson:"valueDifference" json:"valueDifference"`
	TargetWeight     float64 `bson:"targetWeight" json:"targetWeight"`
}

type UserPolicy struct {
	RiskTolerance      float64             `bson:"riskTolerance" json:"riskTolerance"`
	InvestmentHorizon  int                 `bson:"investmentHorizon" json:"investmentHorizon"`
	BrokerType         int                 `bson:"brokerType" json:"brokerType"`
	FilingStatus       string              `bson:"filingStatus" json:"filingStatus"`
	AnnualIncome       float64             `bson:"annualIncome" json:"annualIncome"`
	EquitiesPercent    float64             `bson:"equitiesPercent" json:"equitiesPercent"`
	Categories         map[string][]string `bson:"categories" json:"categories"`
	SectorCaps         map[string]float64  `bson:"sectorCaps" json:"sectorCaps"`
	TargetAllocation   TargetPortfolio     `bson:"targetAllocation" json:"targetAllocation"`
	RebalanceFrequency string              `bson:"rebalanceFrequency" json:"rebalanceFrequency"`
}

type TargetPortfolio struct {
	Name        string          `bson:"name" json:"name"`
	Allocations []TargetHolding `bson:"allocations" json:"allocations"`
	LastUpdated time.Time       `bson:"lastUpdated" json:"lastUpdated"`
}

type TargetHolding struct {
	Symbol       string  `bson:"symbol" json:"symbol"`
	TargetWeight float64 `bson:"targetWeight" json:"targetWeight"`
}

// Add these new types after your existing type declarations
type TokenDocument struct {
	UserID       string    `bson:"user_id"`
	SessionID    string    `bson:"session_id"`
	AccessToken  string    `bson:"access_token"`
	RefreshToken string    `bson:"refresh_token"`
	ExpiryTime   time.Time `bson:"expiry_time"`
}

// Add PerformanceMetrics struct
type PerformanceMetrics struct {
	MeanReturn      float64            `bson:"meanReturn" json:"meanReturn"`
	StdDeviation    float64            `bson:"stdDeviation" json:"stdDeviation"`
	Outperformers   []string           `bson:"outperformers" json:"outperformers"`
	Underperformers []string           `bson:"underperformers" json:"underperformers"`
	ZScores         map[string]float64 `bson:"zScores" json:"zScores"`
}

// Add these new functions for token management
func getStoredTokens(userID string) (*TokenDocument, error) {
	// Load environment variables
	if err := godotenv.Load(".env"); err != nil {
		return nil, fmt.Errorf("error loading .env file: %v", err)
	}

	mongoURI, exists := os.LookupEnv("MONGO_URI")
	if !exists || mongoURI == "" {
		return nil, fmt.Errorf("MONGO_URI environment variable not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)

	collection := client.Database("Integrations").Collection("CoinbaseTokens")

	var tokenDoc TokenDocument
	err = collection.FindOne(ctx, bson.M{"user_id": userID}).Decode(&tokenDoc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to retrieve tokens: %v", err)
	}

	return &tokenDoc, nil
}

func storeTokens(userID string, accessToken string, refreshToken string, expiresIn int) error {
	// Load environment variables
	if err := godotenv.Load(".env"); err != nil {
		return fmt.Errorf("error loading .env file: %v", err)
	}

	mongoURI, exists := os.LookupEnv("MONGO_URI")
	if !exists || mongoURI == "" {
		return fmt.Errorf("MONGO_URI environment variable not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)

	collection := client.Database("Integrations").Collection("CoinbaseTokens")

	// Get the current session ID from the session store
	var sessionID string
	sessionMutex.Lock()
	for id, sess := range sessionStore {
		if sess.UserParams != nil {
			if uid, ok := sess.UserParams["userId"].(string); ok && uid == userID {
				sessionID = id
				break
			}
		}
	}
	sessionMutex.Unlock()

	tokenDoc := TokenDocument{
		UserID:       userID,
		SessionID:    sessionID,
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		ExpiryTime:   time.Now().Add(time.Duration(expiresIn) * time.Second),
	}

	filter := bson.M{"user_id": userID}
	update := bson.M{"$set": tokenDoc}
	opts := options.Update().SetUpsert(true)

	_, err = collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return fmt.Errorf("failed to store tokens: %v", err)
	}

	return nil
}

// fetchCryptoPrice fetches the current price of a cryptocurrency from Polygon.io
func fetchCryptoPrice(symbol string) (float64, error) {
	polygonAPIKey := os.Getenv("POLYGON_API_KEY")
	if polygonAPIKey == "" {
		return 0, fmt.Errorf("POLYGON_API_KEY not set")
	}

	// Format the symbol for Polygon.io (e.g., BTC -> X:BTCUSD)
	formattedSymbol := fmt.Sprintf("X:%sUSD", symbol)

	// Get the current date in YYYY-MM-DD format
	currentDate := time.Now().Format("2006-01-02")

	// Construct the URL for Polygon.io's crypto aggregates endpoint
	url := fmt.Sprintf("https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/%s/%s?apiKey=%s",
		formattedSymbol, currentDate, currentDate, polygonAPIKey)

	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, fmt.Errorf("error creating request: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("error fetching price: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("error response from Polygon.io: %d", resp.StatusCode)
	}

	var result struct {
		Results []struct {
			C float64 `json:"c"` // Closing price
		} `json:"results"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("error decoding response: %v", err)
	}

	if len(result.Results) == 0 {
		return 0, fmt.Errorf("no price data available for %s", symbol)
	}

	return result.Results[0].C, nil
}

// Convert Coinbase holdings to portfolio format
func convertCoinbaseHoldingsToPortfolio(holdingsData []byte, userParams map[string]interface{}) (*Portfolio, error) {
	var coinbaseResp struct {
		Data []struct {
			Balance struct {
				Amount   string `json:"amount"`
				Currency string `json:"currency"`
			} `json:"balance"`
			Name string `json:"name"`
		} `json:"data"`
	}

	if err := json.Unmarshal(holdingsData, &coinbaseResp); err != nil {
		return nil, fmt.Errorf("failed to parse holdings: %v", err)
	}

	holdings := make([]Holding, 0)
	totalValue := 0.0
	totalInvestment := 0.0
	currentTime := time.Now().UTC()

	// Use the user ID directly without hashing again
	userID := getUserParamString(userParams, "userId", "")
	if userID == "" {
		return nil, fmt.Errorf("user ID is required")
	}

	for _, account := range coinbaseResp.Data {
		amount, err := strconv.ParseFloat(account.Balance.Amount, 64)
		if err != nil {
			continue
		}

		if amount > 0 {
			symbol := account.Balance.Currency

			// Get category from userParams or assign default
			category := "Crypto"
			if categories, ok := userParams["categories"].(map[string][]string); ok {
				for cat, symbols := range categories {
					for _, s := range symbols {
						if s == symbol {
							category = cat
							break
						}
					}
				}
			}

			// Fetch current price from Polygon.io
			currentPrice, err := fetchCryptoPrice(symbol)
			if err != nil {
				log.Printf("Warning: Failed to fetch price for %s: %v", symbol, err)
				currentPrice = 1.0 // Fallback to 1.0 if price fetch fails
			}

			// Get cost basis from userParams or use current price * amount
			costBasis := currentPrice * amount // Default to current price * amount if no cost basis provided
			if costBases, ok := userParams["costBasis"].(map[string]float64); ok {
				if cb, exists := costBases[symbol]; exists {
					costBasis = cb
				}
			}

			startMarketValue := costBasis
			endMarketValue := amount * currentPrice

			holding := Holding{
				Symbol:           symbol,
				Name:             account.Name,
				Quantity:         amount,
				CostBasis:        costBasis,
				CurrentPrice:     currentPrice,
				Currency:         "USD",
				Category:         category,
				Beta:             getUserParamFloat(userParams, "betas", 1.0),
				StartMarketValue: startMarketValue,
				EndMarketValue:   endMarketValue,
				RebalancedShares: amount,
				RebalanceCash:    0.0,
				ValueDifference:  endMarketValue - startMarketValue,
				TargetWeight:     0.0,
			}
			holdings = append(holdings, holding)
			totalValue += endMarketValue
			totalInvestment += startMarketValue
		}
	}

	// Initialize performance metrics
	performance := PerformanceMetrics{
		MeanReturn:      0.0,
		StdDeviation:    0.0,
		Outperformers:   []string{},
		Underperformers: []string{},
		ZScores:         make(map[string]float64),
	}

	portfolio := &Portfolio{
		UserID:          userID,
		Name:            getUserParamString(userParams, "name", "Coinbase Portfolio"),
		Holdings:        holdings,
		TotalValue:      totalValue,
		TotalInvestment: totalInvestment,
		StartDate:       getUserParamTime(userParams, "startDate", currentTime),
		EndDate:         currentTime,
		Policy:          createUserPolicy(userParams),
		Performance:     performance,
		LastUpdated:     currentTime,
	}

	return portfolio, nil
}

// Add helper functions for parameter extraction
func getUserParamFloat(params map[string]interface{}, key string, defaultVal float64) float64 {
	if val, ok := params[key]; ok {
		switch v := val.(type) {
		case float64:
			return v
		case int:
			return float64(v)
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f
			}
		}
	}
	return defaultVal
}

func getUserParamString(params map[string]interface{}, key, defaultVal string) string {
	if val, ok := params[key].(string); ok {
		return val
	}
	return defaultVal
}

func getUserParamTime(params map[string]interface{}, key string, defaultVal time.Time) time.Time {
	if val, ok := params[key].(string); ok {
		if t, err := time.Parse(time.RFC3339, val); err == nil {
			return t
		}
	}
	return defaultVal
}

func createUserPolicy(params map[string]interface{}) UserPolicy {
	return UserPolicy{
		BrokerType:         2, // COINBASE = 2
		InvestmentHorizon:  getUserParamInt(params, "investmentHorizon", 2),
		RebalanceFrequency: getUserParamString(params, "rebalanceFrequency", "monthly"),
		RiskTolerance:      getUserParamFloat(params, "riskTolerance", 0.5),
		FilingStatus:       getUserParamString(params, "filingStatus", "Single"),
		AnnualIncome:       getUserParamFloat(params, "annualIncome", 75000.0),
		EquitiesPercent:    getUserParamFloat(params, "equitiesPercent", 1.0),
		Categories:         getUserParamCategories(params),
		SectorCaps:         getUserParamSectorCaps(params),
		TargetAllocation: TargetPortfolio{
			Name:        getUserParamString(params, "name", "Coinbase Default Allocation"),
			LastUpdated: time.Now().UTC(),
			Allocations: getUserParamAllocations(params),
		},
	}
}

// Upload portfolio to MongoDB
func uploadPortfolioToMongo(portfolio *Portfolio) (primitive.ObjectID, error) {
	// Load environment variables
	if err := godotenv.Load(".env"); err != nil {
		return primitive.NilObjectID, fmt.Errorf("error loading .env file: %v", err)
	}

	mongoURI, exists := os.LookupEnv("MONGO_URI")
	if !exists || mongoURI == "" {
		return primitive.NilObjectID, fmt.Errorf("MONGO_URI environment variable not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return primitive.NilObjectID, fmt.Errorf("failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)

	// Start a session
	session, err := client.StartSession()
	if err != nil {
		return primitive.NilObjectID, fmt.Errorf("failed to start session: %v", err)
	}
	defer session.EndSession(ctx)

	// Start a transaction
	var insertedID primitive.ObjectID
	_, err = session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		portfoliosCollection := client.Database("Integrations").Collection("Portfolios")
		userCollection := client.Database("User").Collection("UserInformation")

		// Find existing portfolio
		filter := bson.M{
			"userId":            portfolio.UserID,
			"policy.brokerType": portfolio.Policy.BrokerType,
		}

		var existingPortfolio Portfolio
		err := portfoliosCollection.FindOne(sessCtx, filter).Decode(&existingPortfolio)
		if err != nil && err != mongo.ErrNoDocuments {
			return nil, fmt.Errorf("failed to check for existing portfolio: %v", err)
		}

		if err == mongo.ErrNoDocuments {
			// Insert new portfolio
			result, err := portfoliosCollection.InsertOne(sessCtx, portfolio)
			if err != nil {
				return nil, fmt.Errorf("failed to insert portfolio: %v", err)
			}
			if oid, ok := result.InsertedID.(primitive.ObjectID); ok {
				insertedID = oid
			}
		} else {
			// Update existing portfolio
			portfolio.ID = existingPortfolio.ID // Preserve the original ID
			_, err = portfoliosCollection.ReplaceOne(sessCtx, filter, portfolio)
			if err != nil {
				return nil, fmt.Errorf("failed to update portfolio: %v", err)
			}
			insertedID = existingPortfolio.ID
		}

		// Update UserInformation collection
		userFilter := bson.M{"_id_hash": portfolio.UserID}
		update := bson.M{
			"$addToSet": bson.M{
				"portfolio_ids": insertedID.Hex(),
			},
		}

		_, err = userCollection.UpdateOne(sessCtx, userFilter, update)
		if err != nil {
			return nil, fmt.Errorf("failed to update user portfolio IDs: %v", err)
		}

		return nil, nil
	})

	if err != nil {
		return primitive.NilObjectID, err
	}

	return insertedID, nil
}

// Produce portfolio to Kafka
func produceToKafka(portfolio *Portfolio) error {
	// Before marshaling, ensure all time fields have timezone information
	portfolio.LastUpdated = portfolio.LastUpdated.UTC()
	portfolio.Policy.TargetAllocation.LastUpdated = portfolio.Policy.TargetAllocation.LastUpdated.UTC()

	// Marshal the portfolio
	portfolioJSON, err := json.Marshal(portfolio)
	if err != nil {
		return fmt.Errorf("failed to marshal portfolio: %v", err)
	}

	kafkaURL := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	kafkaKey := os.Getenv("KAFKA_KEY")
	kafkaSecret := os.Getenv("KAFKA_SECRET")

	if kafkaURL == "" || kafkaKey == "" || kafkaSecret == "" {
		return fmt.Errorf("Kafka environment variables not properly set")
	}

	// Set up the SASL mechanism
	mechanism := &plain.Mechanism{
		Username: kafkaKey,
		Password: kafkaSecret,
	}

	// Set up TLS configuration
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// Create a dialer with SASL/PLAIN over TLS
	dialer := &kafka.Dialer{
		SASLMechanism: mechanism,
		TLS:           tlsConfig,
	}

	// Configure the writer
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaURL},
		Topic:   "alert_rebalance",
		Dialer:  dialer,
		// Add additional configurations for reliability
		RequiredAcks: -1, // -1 means all replicas must acknowledge (equivalent to RequireAll)
		MaxAttempts:  3,
	})
	defer writer.Close()

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Write the message
	err = writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(portfolio.UserID), // Use UserID as the message key
		Value: portfolioJSON,
		Time:  time.Now(),
	})

	if err != nil {
		return fmt.Errorf("failed to write message to Kafka: %v", err)
	}

	return nil
}

// Update the holdingsHandler
func holdingsHandler(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("session_id")
	if err != nil || cookie.Value == "" {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	// Get session and user parameters
	sessionMutex.Lock()
	sess, exists := sessionStore[cookie.Value]
	sessionMutex.Unlock()
	if !exists {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	// Get user ID from session parameters
	userID := getUserParamString(sess.UserParams, "userId", "")
	if userID == "" {
		http.Error(w, "No user ID found in session parameters", http.StatusBadRequest)
		return
	}

	// Try to get tokens from MongoDB first using the actual user ID
	tokenDoc, err := getStoredTokens(userID)
	if err != nil {
		log.Printf("Error retrieving stored tokens: %v", err)
		// Fall back to session tokens
		if sess.AccessToken == "" {
			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
		tokenDoc = &TokenDocument{
			AccessToken:  sess.AccessToken,
			RefreshToken: sess.RefreshToken,
		}
	}

	// Check if token is expired and needs refresh
	if tokenDoc.ExpiryTime.Before(time.Now()) {
		// Implement refresh logic here
		// For now, redirect to re-authenticate
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	// Get user parameters from session
	sessionMutex.Lock()
	sess, exists = sessionStore[cookie.Value]
	sessionMutex.Unlock()
	if !exists {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	// Use user parameters from session
	userParams := sess.UserParams
	if userParams == nil {
		userParams = make(map[string]interface{})
	}

	// Get holdings from Coinbase
	client := &http.Client{}
	req, err := http.NewRequest("GET", coinbaseAccountsURL, nil)
	if err != nil {
		http.Error(w, "Error creating request: "+err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("Authorization", "Bearer "+tokenDoc.AccessToken)

	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, "Error fetching account holdings: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, "Error reading holdings response: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Update portfolio conversion to use user parameters
	portfolio, err := convertCoinbaseHoldingsToPortfolio(body, userParams)
	if err != nil {
		http.Error(w, "Error converting holdings: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Upload to MongoDB
	portfolioID, err := uploadPortfolioToMongo(portfolio)
	if err != nil {
		http.Error(w, "Error uploading to MongoDB: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Produce to Kafka
	if err := produceToKafka(portfolio); err != nil {
		log.Printf("Warning: Failed to produce to Kafka: %v", err)
		// Continue anyway as this is not critical
	}

	// Return success response with both processed and raw data
	response := map[string]interface{}{
		"success":        true,
		"message":        "Coinbase connection successful",
		"portfolio_id":   portfolioID.Hex(),
		"holdings_count": len(portfolio.Holdings),
		"total_value":    portfolio.TotalValue,
		"raw_data":       string(body),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func createMongoIndexes() error {
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("MONGO_URI")))
	if err != nil {
		return err
	}
	defer client.Disconnect(ctx)

	collection := client.Database("Integrations").Collection("CoinbaseTokens")
	_, err = collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "userId", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	return err
}

// Add these helper functions after the existing getUserParam functions

func getUserParamInt(params map[string]interface{}, key string, defaultVal int) int {
	if val, ok := params[key]; ok {
		switch v := val.(type) {
		case int:
			return v
		case float64:
			return int(v)
		case string:
			if i, err := strconv.Atoi(v); err == nil {
				return i
			}
		}
	}
	return defaultVal
}

func getUserParamCategories(params map[string]interface{}) map[string][]string {
	defaultCategories := map[string][]string{
		"Crypto":         []string{},
		"Stablecoins":    []string{},
		"DeFi":           []string{},
		"Smart Contract": []string{},
		"Layer2":         []string{},
		"Gaming":         []string{},
	}

	if categories, ok := params["categories"].(map[string]interface{}); ok {
		result := make(map[string][]string)
		for cat, symbols := range categories {
			if symbolList, ok := symbols.([]interface{}); ok {
				strSymbols := make([]string, 0, len(symbolList))
				for _, sym := range symbolList {
					if strSym, ok := sym.(string); ok {
						strSymbols = append(strSymbols, strSym)
					}
				}
				result[cat] = strSymbols
			}
		}
		return result
	}
	return defaultCategories
}

func getUserParamSectorCaps(params map[string]interface{}) map[string]float64 {
	defaultCaps := map[string]float64{
		"Crypto":         0.40,
		"Stablecoins":    0.30,
		"DeFi":           0.20,
		"Smart Contract": 0.25,
		"Layer2":         0.15,
		"Gaming":         0.10,
	}

	if caps, ok := params["sectorCaps"].(map[string]interface{}); ok {
		result := make(map[string]float64)
		for sector, cap := range caps {
			switch v := cap.(type) {
			case float64:
				result[sector] = v
			case int:
				result[sector] = float64(v)
			case string:
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					result[sector] = f
				}
			}
		}
		return result
	}
	return defaultCaps
}

func getUserParamAllocations(params map[string]interface{}) []TargetHolding {
	if allocs, ok := params["allocations"].([]interface{}); ok {
		result := make([]TargetHolding, 0, len(allocs))
		for _, alloc := range allocs {
			if allocMap, ok := alloc.(map[string]interface{}); ok {
				symbol, symbolOk := allocMap["symbol"].(string)
				weight, weightOk := allocMap["targetWeight"].(float64)
				if symbolOk && weightOk {
					result = append(result, TargetHolding{
						Symbol:       symbol,
						TargetWeight: weight,
					})
				}
			}
		}
		return result
	}
	return []TargetHolding{}
}

func main() {
	// Seed the random number generator for session IDs.
	rand.Seed(time.Now().UnixNano())

	// Ensure required environment variables are set.
	if clientID == "" || clientSecret == "" || redirectURI == "" {
		log.Fatal("COINBASE_OAUTH_CLIENT_ID, COINBASE_OAUTH_SECRET, and COINBASE_REDIRECT_URI must be set")
	}

	// Set up HTTP handlers with CORS middleware
	http.HandleFunc("/", corsMiddleware(indexHandler))
	http.HandleFunc("/callback", corsMiddleware(callbackHandler))
	http.HandleFunc("/holdings", corsMiddleware(holdingsHandler))

	// Start the server with TLS on port 6070.
	fmt.Println("Server starting on :6070 with HTTPS")
	log.Fatal(http.ListenAndServeTLS(":6070", "server.crt", "server.key", nil))
}
