package models

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Portfolio represents an actual portfolio snapshot along with the user's policy.
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

// Holding represents a single asset within the portfolio.
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

// PerformanceMetrics represents portfolio performance analysis data
type PerformanceMetrics struct {
	MeanReturn      float64            `bson:"meanReturn" json:"meanReturn"`
	StdDeviation    float64            `bson:"stdDeviation" json:"stdDeviation"`
	Outperformers   []string           `bson:"outperformers" json:"outperformers"`
	Underperformers []string           `bson:"underperformers" json:"underperformers"`
	ZScores         map[string]float64 `bson:"zScores" json:"zScores"`
}

// InvestmentHorizon represents the investment duration as an enum.
type InvestmentHorizon int

const (
	ShortTerm  InvestmentHorizon = iota // e.g., less than 3 years
	MediumTerm                          // e.g., 3-10 years
	LongTerm                            // e.g., more than 10 years
)

func (ih InvestmentHorizon) String() string {
	switch ih {
	case ShortTerm:
		return "ShortTerm"
	case MediumTerm:
		return "MediumTerm"
	case LongTerm:
		return "LongTerm"
	default:
		return "Unknown"
	}
}

// BrokerType represents the type of brokerage used by the user.
type BrokerType int

const (
	ETRADE BrokerType = iota
	SCHWAB
	COINBASE
	MANUAL
)

func (bt BrokerType) String() string {
	switch bt {
	case ETRADE:
		return "ETRADE"
	case SCHWAB:
		return "SCHWAB"
	case COINBASE:
		return "COINBASE"
	case MANUAL:
		return "MANUAL"
	default:
		return "UNKNOWN"
	}
}

func (bt *BrokerType) UnmarshalJSON(data []byte) error {
	// First, try to parse data as a string.
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		switch s {
		case "ETRADE":
			*bt = ETRADE
		case "SCHWAB":
			*bt = SCHWAB
		case "COINBASE":
			*bt = COINBASE
		case "MANUAL":
			*bt = MANUAL
		default:
			return fmt.Errorf("unknown broker type: %s", s)
		}
		return nil
	}

	// Next, try to parse data as a number.
	var n float64
	if err := json.Unmarshal(data, &n); err == nil {
		switch int(n) {
		case int(ETRADE):
			*bt = ETRADE
		case int(SCHWAB):
			*bt = SCHWAB
		case int(COINBASE):
			*bt = COINBASE
		case int(MANUAL):
			*bt = MANUAL
		default:
			return fmt.Errorf("unknown broker type: %d", int(n))
		}
		return nil
	}

	// Next, try to parse data as a number in the format {"$numberInt": "3"}.
	var mongoDoc map[string]interface{}
	if err := json.Unmarshal(data, &mongoDoc); err == nil {
		if numInt, ok := mongoDoc["$numberInt"]; ok {
			if strVal, ok := numInt.(string); ok {
				if val, err := strconv.Atoi(strVal); err == nil {
					switch val {
					case int(ETRADE):
						*bt = ETRADE
					case int(SCHWAB):
						*bt = SCHWAB
					case int(COINBASE):
						*bt = COINBASE
					case int(MANUAL):
						*bt = MANUAL
					default:
						return fmt.Errorf("unknown broker type: %d", val)
					}
					return nil
				}
			}
		}
		return nil
	}

	return fmt.Errorf("unknown broker type: %v", data)
}

// UserPolicy represents the user's portfolio management preferences.
type UserPolicy struct {
	RiskTolerance      float64             `bson:"riskTolerance" json:"riskTolerance"`
	InvestmentHorizon  InvestmentHorizon   `bson:"investmentHorizon" json:"investmentHorizon"`
	BrokerType         BrokerType          `bson:"brokerType" json:"brokerType"`
	TargetAllocation   TargetPortfolio     `bson:"targetAllocation" json:"targetAllocation"`
	RebalanceFrequency string              `bson:"rebalanceFrequency" json:"rebalanceFrequency"`
	FilingStatus       string              `bson:"filingStatus" json:"filingStatus"`
	AnnualIncome       float64             `bson:"annualIncome" json:"annualIncome"`
	EquitiesPercent    float64             `bson:"equitiesPercent" json:"equitiesPercent"`
	Categories         map[string][]string `bson:"categories" json:"categories"`
	SectorCaps         map[string]float64  `bson:"sectorCaps" json:"sectorCaps"`
}

// TargetPortfolio represents the desired asset allocation.
type TargetPortfolio struct {
	UserID      string          `bson:"userId" json:"userId"`
	Name        string          `bson:"name" json:"name"`
	Allocations []TargetHolding `bson:"allocations" json:"allocations"`
	LastUpdated time.Time       `bson:"lastUpdated" json:"lastUpdated"`
}

// TargetHolding represents the desired allocation for an asset or asset class.
type TargetHolding struct {
	Symbol       string  `bson:"symbol" json:"symbol"`
	TargetWeight float64 `bson:"targetWeight" json:"targetWeight"`
}
