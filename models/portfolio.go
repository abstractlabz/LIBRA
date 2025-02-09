package models

import (
	"encoding/json"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Portfolio represents an actual portfolio snapshot along with the user's policy.
type Portfolio struct {
	ID          primitive.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	UserID      string             `bson:"userId" json:"userId"`
	Name        string             `bson:"name" json:"name"`
	Holdings    []Holding          `bson:"holdings" json:"holdings"`
	Policy      UserPolicy         `bson:"policy" json:"policy"` // Embedded user policy
	LastUpdated time.Time          `bson:"lastUpdated" json:"lastUpdated"`
}

// Holding represents a single asset within the portfolio.
type Holding struct {
	Symbol       string  `bson:"symbol" json:"symbol"`             // e.g., "AAPL"
	Name         string  `bson:"name" json:"name"`                 // e.g., "Apple Inc."
	Quantity     float64 `bson:"quantity" json:"quantity"`         // Number of shares/units held
	CostBasis    float64 `bson:"costBasis" json:"costBasis"`       // Cost basis per share/unit
	CurrentPrice float64 `bson:"currentPrice" json:"currentPrice"` // Latest market price
	Currency     string  `bson:"currency" json:"currency"`         // Currency code, e.g., "USD"
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
	ROBINHOOD
	MANUAL
)

func (bt BrokerType) String() string {
	switch bt {
	case ETRADE:
		return "ETRADE"
	case SCHWAB:
		return "SCHWAB"
	case ROBINHOOD:
		return "ROBINHOOD"
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
		case "ROBINHOOD":
			*bt = ROBINHOOD
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
		case int(ROBINHOOD):
			*bt = ROBINHOOD
		case int(MANUAL):
			*bt = MANUAL
		default:
			return fmt.Errorf("unknown broker type: %d", int(n))
		}
		return nil
	}

	return fmt.Errorf("unknown broker type: %v", data)
}

// UserPolicy represents the user's portfolio management preferences.
type UserPolicy struct {
	UserID             string            `bson:"userId" json:"userId"`
	RiskTolerance      float64           `bson:"riskTolerance" json:"riskTolerance"`           // e.g., 0.0 (low) to 1.0 (high)
	InvestmentHorizon  InvestmentHorizon `bson:"investmentHorizon" json:"investmentHorizon"`   // ShortTerm, MediumTerm, or LongTerm
	BrokerType         BrokerType        `bson:"brokerType" json:"brokerType"`                 // Broker type as a custom enum (with custom unmarshal logic)
	TargetAllocation   TargetPortfolio   `bson:"targetAllocation" json:"targetAllocation"`     // The desired portfolio allocation
	RebalanceFrequency string            `bson:"rebalanceFrequency" json:"rebalanceFrequency"` // e.g., "monthly", "quarterly"
	UserName           string            `bson:"userName" json:"userName"`
	UserPass           string            `bson:"userPass" json:"userPass"`
}

// TargetPortfolio represents the desired asset allocation.
type TargetPortfolio struct {
	UserID      string          `bson:"userId" json:"userId"`
	Name        string          `bson:"name" json:"name"` // e.g., "Aggressive Growth Allocation"
	Allocations []TargetHolding `bson:"allocations" json:"allocations"`
	LastUpdated time.Time       `bson:"lastUpdated" json:"lastUpdated"`
}

// TargetHolding represents the desired allocation for an asset or asset class.
type TargetHolding struct {
	Symbol       string  `bson:"symbol" json:"symbol"`             // e.g., "AAPL" or "Stocks"
	TargetWeight float64 `bson:"targetWeight" json:"targetWeight"` // e.g., 0.6 for 60%
}
