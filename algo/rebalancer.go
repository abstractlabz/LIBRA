package main

import (
	"encoding/json"
	"log"
	"strconv"

	"github.com/0xPCDefenders/LIBRA/models"
	"github.com/0xPCDefenders/LIBRA/utils"
	"github.com/segmentio/kafka-go"
)

// Local struct definitions to use when building the Portfolio
type LocalPolicy struct {
	RiskTolerance    float64
	TargetAllocation LocalTargetAllocation
}

type LocalTargetAllocation struct {
	Allocations []LocalAllocation
}

type LocalAllocation struct {
	Symbol       string
	TargetWeight float64
}

// emptyProcess processes a single portfolio JSON message by performing
// rebalancing similar to the logic in rebalancer.py.
func emptyProcess(portfolio models.Portfolio) {
	log.Printf("Processing portfolio for user %s with name %s and %d holdings",
		portfolio.UserID, portfolio.Name, len(portfolio.Holdings))

	// === STEP 1: Calculate total investment based on holdings' costBasis and quantity.
	totalInvestment := 0.0
	for _, holding := range portfolio.Holdings {
		totalInvestment += holding.Quantity * holding.CostBasis
	}
	if totalInvestment <= 0 {
		totalInvestment = 10000.0 // default if no valid value was computed
	}

	log.Printf("Total investment (start value): $%.2f", totalInvestment)

	// === STEP 2: Calculate total current portfolio value using each holding's currentPrice.
	totalCurrentValue := 0.0
	for _, holding := range portfolio.Holdings {
		totalCurrentValue += holding.Quantity * holding.CurrentPrice
	}
	log.Printf("Total current portfolio value: $%.2f", totalCurrentValue)

	// === STEP 3: Determine risk preference from policy.
	// (In your Python code, riskTolerance on a 0-1 scale is multiplied by 2.)
	riskTolerance := portfolio.Policy.RiskTolerance
	riskPreference := riskTolerance * 2.0
	log.Printf("Risk tolerance (0-1): %.2f, risk preference (0-2): %.2f", riskTolerance, riskPreference)

	// === STEP 4: Build a map of target allocations from the policy.
	// (Assumes portfolio.Policy.TargetAllocation.Allocations exists.)
	targetAllocations := make(map[string]float64)
	for _, alloc := range portfolio.Policy.TargetAllocation.Allocations {
		targetAllocations[alloc.Symbol] = alloc.TargetWeight
	}
	log.Println("Target allocations from policy:")
	for symbol, weight := range targetAllocations {
		log.Printf("  %s: %.2f%%", symbol, weight*100)
	}

	// === STEP 5: Rebalance the portfolio.
	// For each holding (that is present in the target allocations), compute:
	//   - target dollar value = (targetWeight * totalCurrentValue)
	//   - new number of shares = target dollar value / currentPrice
	//   - rebalance cash flow = (target dollar value - current market value)
	rebalanceResults := make(map[string]struct {
		NewShares     float64
		RebalanceCash float64
		CurrentValue  float64
		TargetValue   float64
	})
	for symbol, targetWeight := range targetAllocations {
		// Find the matching holding
		var currentHolding *models.Holding
		for i := range portfolio.Holdings {
			if portfolio.Holdings[i].Symbol == symbol {
				currentHolding = &portfolio.Holdings[i]
				break
			}
		}
		if currentHolding == nil {
			log.Printf("Warning: no current holding found for symbol %s; skipping rebalancing for this allocation", symbol)
			continue
		}

		// Calculate current market value for the holding.
		currentValue := currentHolding.Quantity * currentHolding.CurrentPrice

		// Compute the target value for this holding.
		targetValue := targetWeight * totalCurrentValue

		// Calculate new shares required to meet target allocation.
		newShares := targetValue / currentHolding.CurrentPrice

		// Compute the cash flow needed (positive means buying, negative means selling).
		rebalanceCash := targetValue - currentValue

		// Save the result.
		rebalanceResults[symbol] = struct {
			NewShares     float64
			RebalanceCash float64
			CurrentValue  float64
			TargetValue   float64
		}{
			NewShares:     newShares,
			RebalanceCash: rebalanceCash,
			CurrentValue:  currentValue,
			TargetValue:   targetValue,
		}

		// Update the holding's quantity to the new value.
		currentHolding.Quantity = newShares
	}

	// === STEP 6: Log rebalancing results.
	log.Println("Rebalancing Results:")
	for symbol, res := range rebalanceResults {
		log.Printf("Symbol: %s | New Shares: %.4f | Current Value: $%.2f | Target Value: $%.2f | Cash Flow: $%.2f",
			symbol, res.NewShares, res.CurrentValue, res.TargetValue, res.RebalanceCash)
	}

	// === STEP 7: Display final portfolio details after rebalancing.
	log.Println("Final portfolio after rebalancing:")
	for _, h := range portfolio.Holdings {
		marketValue := h.Quantity * h.CurrentPrice
		log.Printf("Symbol: %s | Shares: %.4f | Current Price: $%.2f | Market Value: $%.2f",
			h.Symbol, h.Quantity, h.CurrentPrice, marketValue)
	}
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
			// Print the raw message for debugging
			log.Printf("Received message: %s", string(m.Value))

			var p models.Portfolio
			if err := json.Unmarshal(m.Value, &p); err != nil {
				log.Printf("Failed to unmarshal portfolio: %v", err)

				// Try to determine what format the message is in
				var rawMsg map[string]interface{}
				if jsonErr := json.Unmarshal(m.Value, &rawMsg); jsonErr == nil {
					log.Printf("Message appears to be JSON but not a Portfolio. Content: %v", rawMsg)

					// Create a portfolio from the parsed map
					portfolio := models.Portfolio{
						UserID: getString(rawMsg, "userId"),
						Name:   getString(rawMsg, "name"),
					}

					// Extract holdings
					if holdingsArr, ok := rawMsg["holdings"].([]interface{}); ok {
						for _, h := range holdingsArr {
							if holdingMap, ok := h.(map[string]interface{}); ok {
								portfolio.Holdings = append(portfolio.Holdings, models.Holding{
									Symbol:       getString(holdingMap, "symbol"),
									Name:         getString(holdingMap, "name"),
									Quantity:     getNumber(holdingMap, "quantity"),
									CostBasis:    getNumber(holdingMap, "costBasis"),
									CurrentPrice: getNumber(holdingMap, "currentPrice"),
								})
							}
						}
					}

					// Extract policy and allocations using local structs first
					var localPolicy LocalPolicy
					if policyMap, ok := rawMsg["policy"].(map[string]interface{}); ok {
						// Extract risk tolerance
						localPolicy.RiskTolerance = getMongoDouble(policyMap, "riskTolerance")
						portfolio.Policy.RiskTolerance = localPolicy.RiskTolerance

						// Extract broker type
						brokerTypeInt := getMongoInt(policyMap, "brokerType")
						switch brokerTypeInt {
						case 0:
							portfolio.Policy.BrokerType = models.ETRADE
						case 1:
							portfolio.Policy.BrokerType = models.SCHWAB
						case 2:
							portfolio.Policy.BrokerType = models.ROBINHOOD
						case 3:
							portfolio.Policy.BrokerType = models.MANUAL
						}

						// Extract investment horizon
						horizonInt := getMongoInt(policyMap, "investmentHorizon")
						switch horizonInt {
						case 0:
							portfolio.Policy.InvestmentHorizon = models.ShortTerm
						case 1:
							portfolio.Policy.InvestmentHorizon = models.MediumTerm
						case 2:
							portfolio.Policy.InvestmentHorizon = models.LongTerm
						}

						// Extract rebalance frequency
						portfolio.Policy.RebalanceFrequency = getString(policyMap, "rebalanceFrequency")

						// Extract target allocation
						if taMap, ok := policyMap["targetAllocation"].(map[string]interface{}); ok {
							portfolio.Policy.TargetAllocation.Name = getString(taMap, "name")

							if allocArr, ok := taMap["allocations"].([]interface{}); ok {
								for _, a := range allocArr {
									if allocMap, ok := a.(map[string]interface{}); ok {
										allocation := models.TargetHolding{
											Symbol:       getString(allocMap, "symbol"),
											TargetWeight: getMongoDouble(allocMap, "targetWeight"),
										}
										portfolio.Policy.TargetAllocation.Allocations = append(
											portfolio.Policy.TargetAllocation.Allocations, allocation)
									}
								}
							}
						}
					}

					// Ensure the holdings aren't nil
					if len(portfolio.Holdings) > 0 {
						// Try to process using what we have
						log.Println("Successfully built portfolio from MongoDB JSON format")
						emptyProcess(portfolio)
						return
					}

					log.Printf("Failed to build complete portfolio. Holdings: %d, Risk: %f",
						len(portfolio.Holdings),
						localPolicy.RiskTolerance)
				}
				return
			}
			emptyProcess(p)
		}(msg)
	}
}

// Helper functions to extract values from various formats

// getString extracts a string from a map
func getString(data map[string]interface{}, key string) string {
	if val, ok := data[key].(string); ok {
		return val
	}
	return ""
}

// getNumber extracts a number from a map
func getNumber(data map[string]interface{}, key string) float64 {
	if val, ok := data[key].(float64); ok {
		return val
	}
	return 0
}

// getMongoDouble extracts a MongoDB $numberDouble value
func getMongoDouble(data map[string]interface{}, key string) float64 {
	if val, ok := data[key].(map[string]interface{}); ok {
		if numStr, ok := val["$numberDouble"].(string); ok {
			if f, err := strconv.ParseFloat(numStr, 64); err == nil {
				return f
			}
		}
	}
	return getNumber(data, key) // fallback
}

// getMongoInt extracts a MongoDB $numberInt value
func getMongoInt(data map[string]interface{}, key string) int {
	if val, ok := data[key].(map[string]interface{}); ok {
		if numStr, ok := val["$numberInt"].(string); ok {
			if i, err := strconv.Atoi(numStr); err == nil {
				return i
			}
		}
	}
	// Try to convert a plain number as fallback
	if val, ok := data[key].(float64); ok {
		return int(val)
	}
	return 0 // default
}

func main() {
	log.Println("Starting rebalancer...")
	consumeAlertRebalance()
}
