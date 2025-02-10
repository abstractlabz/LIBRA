package main

import (
	"encoding/json"
	"log"

	"github.com/0xPCDefenders/LIBRA/models"
	"github.com/0xPCDefenders/LIBRA/utils"
	"github.com/segmentio/kafka-go"
)

// emptyProcess processes a single portfolio JSON message by performing
// rebalancing similar to the logic in rebalancer.py.
func emptyProcess(portfolio models.Portfolio) {
	log.Printf("Processing portfolio for user %s with name %s and %d holdings",
		portfolio.UserID, portfolio.Name, len(portfolio.Holdings))

	// === STEP 1: Calculate total investment based on holdings’ costBasis and quantity.
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
