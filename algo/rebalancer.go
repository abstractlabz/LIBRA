package main

import (
	"encoding/json"
	"log"
	"math"
	"time"

	"github.com/0xPCDefenders/LIBRA/models"
	"github.com/0xPCDefenders/LIBRA/utils"
	"github.com/segmentio/kafka-go"
)

const (
	RISK_FREE_RATE = 0.04306 // 4.306% risk-free rate assumption
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

// Add this custom time type and its methods
type JSONTime time.Time

func (t *JSONTime) UnmarshalJSON(b []byte) error {
	// Remove quotes from string
	s := string(b[1 : len(b)-1])

	// Try parsing with different formats
	formats := []string{
		time.RFC3339,
		"2006-01-02T15:04:05.999999",
		"2006-01-02T15:04:05",
	}

	var err error
	for _, format := range formats {
		parsed, parseErr := time.Parse(format, s)
		if parseErr == nil {
			*t = JSONTime(parsed)
			return nil
		}
		err = parseErr
	}
	return err
}

// processPortfolio implements the enhanced portfolio optimization logic
func processPortfolio(portfolio models.Portfolio) {
	log.Printf("Processing portfolio for user %s with name %s and %d holdings",
		portfolio.UserID, portfolio.Name, len(portfolio.Holdings))

	// Calculate initial portfolio metrics
	calculatePortfolioMetrics(&portfolio)

	// Perform risk analysis and optimization
	optimizePortfolio(&portfolio)

	// Rebalance the portfolio
	rebalancePortfolio(&portfolio)

	// Calculate tax implications
	calculateTaxImplications(&portfolio)

	// Update performance metrics
	updatePerformanceMetrics(&portfolio)

	// Produce the updated portfolio to alert_optimize topic
	producePortfolio(portfolio)
}

func calculatePortfolioMetrics(p *models.Portfolio) {
	totalInvestment := 0.0
	totalCurrentValue := 0.0

	for i := range p.Holdings {
		holding := &p.Holdings[i]

		// Calculate market values
		holding.StartMarketValue = holding.Quantity * holding.CostBasis
		holding.EndMarketValue = holding.Quantity * holding.CurrentPrice

		totalInvestment += holding.StartMarketValue
		totalCurrentValue += holding.EndMarketValue

		// Calculate value difference
		holding.ValueDifference = holding.EndMarketValue - holding.StartMarketValue
	}

	p.TotalInvestment = totalInvestment
	p.TotalValue = totalCurrentValue
}

func optimizePortfolio(p *models.Portfolio) {
	riskPreference := p.Policy.RiskTolerance * 2.0 // Scale 0-1 to 0-2

	// Calculate weights based on risk preference and sector caps
	weights := calculateOptimalWeights(p.Holdings, riskPreference, p.Policy.SectorCaps)

	// Apply weights to holdings
	for i := range p.Holdings {
		holding := &p.Holdings[i]
		if weight, exists := weights[holding.Symbol]; exists {
			holding.TargetWeight = weight
		}
	}
}

func calculateOptimalWeights(holdings []models.Holding, riskPreference float64, sectorCaps map[string]float64) map[string]float64 {
	weights := make(map[string]float64)
	sectorWeights := make(map[string]float64)

	// First pass: calculate initial weights based on beta alignment
	for _, holding := range holdings {
		weight := calculateWeightFactor(holding.Beta, riskPreference)
		weights[holding.Symbol] = weight
		sectorWeights[holding.Category] += weight
	}

	// Second pass: adjust for sector caps
	for _, holding := range holdings {
		sectorCap := sectorCaps[holding.Category]
		if sectorWeights[holding.Category] > sectorCap {
			adjustment := sectorCap / sectorWeights[holding.Category]
			weights[holding.Symbol] *= adjustment
		}
	}

	// Normalize weights to sum to 1.0
	normalizeWeights(weights)

	return weights
}

func calculateWeightFactor(beta, riskPreference float64) float64 {
	var targetBeta float64
	if riskPreference > 1.5 {
		targetBeta = 1.5 + (math.Exp(riskPreference-1.5)-1)*0.5
	} else {
		targetBeta = riskPreference
	}

	distance := math.Abs(beta - targetBeta)
	weightFactor := math.Exp(-2 * distance)

	if riskPreference > 1.5 && beta > 1.5 {
		weightFactor *= 1.5
	}

	return weightFactor
}

func normalizeWeights(weights map[string]float64) {
	total := 0.0
	for _, weight := range weights {
		total += weight
	}

	if total > 0 {
		for symbol := range weights {
			weights[symbol] /= total
		}
	}
}

func rebalancePortfolio(p *models.Portfolio) {
	for i := range p.Holdings {
		holding := &p.Holdings[i]

		// Calculate target value based on optimal weight
		targetValue := holding.TargetWeight * p.TotalValue

		// Calculate new shares needed
		if holding.CurrentPrice > 0 {
			holding.RebalancedShares = targetValue / holding.CurrentPrice
			holding.RebalanceCash = targetValue - holding.EndMarketValue
		}
	}
}

func calculateTaxImplications(p *models.Portfolio) {
	// Convert JSONTime to time.Time for calculations
	startTime := time.Time(p.StartDate)
	endTime := time.Time(p.EndDate)
	holdingPeriod := endTime.Sub(startTime)
	isLongTerm := holdingPeriod.Hours() > 24*365

	for i := range p.Holdings {
		holding := &p.Holdings[i]
		if holding.RebalanceCash < 0 { // Selling shares
			gain := -holding.RebalanceCash // Use absolute value
			var taxRate float64

			if isLongTerm {
				// Apply long-term capital gains rates based on income
				switch {
				case p.Policy.AnnualIncome < 44475:
					taxRate = 0.0 // 0% tax rate
				case p.Policy.AnnualIncome < 492300:
					taxRate = 0.15 // 15% tax rate
				default:
					taxRate = 0.20 // 20% tax rate
				}
			} else {
				// Short-term gains are taxed as ordinary income
				// This is simplified - implement full tax bracket logic
				taxRate = 0.24 // Example tax rate
			}

			// Calculate tax impact and update value difference
			taxAmount := gain * taxRate
			holding.ValueDifference = holding.EndMarketValue - holding.StartMarketValue - taxAmount
		}
	}
}

func updatePerformanceMetrics(p *models.Portfolio) {
	returns := make([]float64, 0, len(p.Holdings))
	holdingReturns := make(map[string]float64)

	// Calculate returns for each holding
	for _, holding := range p.Holdings {
		if holding.StartMarketValue > 0 {
			ret := (holding.EndMarketValue - holding.StartMarketValue) / holding.StartMarketValue
			returns = append(returns, ret)
			holdingReturns[holding.Symbol] = ret
		}
	}

	// Calculate mean return
	p.Performance.MeanReturn = calculateMean(returns)

	// Calculate standard deviation
	p.Performance.StdDeviation = calculateStdDev(returns, p.Performance.MeanReturn)

	// Calculate z-scores and identify outperformers/underperformers
	p.Performance.ZScores = make(map[string]float64)
	p.Performance.Outperformers = []string{}
	p.Performance.Underperformers = []string{}

	// Only calculate z-scores if we have a valid standard deviation
	if p.Performance.StdDeviation > 0 {
		for symbol, ret := range holdingReturns {
			zScore := (ret - p.Performance.MeanReturn) / p.Performance.StdDeviation
			// Check for NaN before assigning
			if !math.IsNaN(zScore) {
				p.Performance.ZScores[symbol] = zScore

				if zScore > 0.5 {
					p.Performance.Outperformers = append(p.Performance.Outperformers, symbol)
				} else if zScore < -0.5 {
					p.Performance.Underperformers = append(p.Performance.Underperformers, symbol)
				}
			}
		}
	}
}

func calculateMean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func calculateStdDev(values []float64, mean float64) float64 {
	if len(values) < 2 {
		return 0
	}
	sumSquares := 0.0
	for _, v := range values {
		sumSquares += (v - mean) * (v - mean)
	}
	variance := sumSquares / float64(len(values)-1)
	if variance <= 0 {
		return 0
	}
	return math.Sqrt(variance)
}

func producePortfolio(portfolio models.Portfolio) {
	portfolioJSON, err := json.Marshal(portfolio)
	if err != nil {
		log.Printf("Failed to marshal portfolio: %v", err)
		return
	}
	utils.ProduceMessage(string(portfolioJSON), "alert_optimize")
	log.Println("Successfully produced portfolio to alert_optimize topic")
}

func consumeAlertRebalance() {
	buffer := make(chan kafka.Message, 100)
	go utils.ConsumeToBuffer(buffer, "alert_rebalance", "rebalancer-group", "../.env")

	log.Println("Rebalancer is listening to the alert_rebalance topic...")

	for msg := range buffer {
		go func(m kafka.Message) {
			var portfolio models.Portfolio
			if err := json.Unmarshal(m.Value, &portfolio); err != nil {
				log.Printf("Failed to unmarshal portfolio: %v", err)
				return
			}
			processPortfolio(portfolio)
		}(msg)
	}
}

func main() {
	log.Println("Starting enhanced portfolio rebalancer...")
	consumeAlertRebalance()
}
