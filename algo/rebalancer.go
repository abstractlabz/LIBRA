package main

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Global variables and constants

var categories = map[string][]string{
	"Tech":           {"AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META"},
	"Financials":     {"JPM", "BAC", "V", "MA"},
	"Consumer Goods": {"WMT", "PG", "KO", "PEP"},
	"Energy":         {"XOM", "CVX"},
	"Entertainment":  {"DIS", "NFLX"},
	"Industrials":    {"CAT", "GE"},
}

var portfolioStructure = make(map[string]map[string]float64) // category -> ticker -> weight
var portfolio = make(map[string]map[string]*Position)        // category -> ticker -> position

var USER_TAX_BRACKET float64
var totalInvestment float64
var equitiesPercentage float64 = 1.0

const TRANSACTION_COST = 0.001 // 0.1%

var incomeBrackets = map[int]string{
	1: "0-11,000 (10%)",
	2: "11,000-44,725 (12%)",
	3: "44,725-95,375 (22%)",
	4: "95,375-182,100 (24%)",
	5: "182,100-231,250 (32%)",
	6: "231,250-578,125 (35%)",
	7: "578,125+ (37%)",
}

var taxBracketMapping = map[int]float64{
	1: 0.10,
	2: 0.12,
	3: 0.22,
	4: 0.24,
	5: 0.32,
	6: 0.35,
	7: 0.37,
}

// Global variable to store the user's selected bracket number
var USER_TAX_BRACKET_NUM int

// Position represents an asset holding.
type Position struct {
	Shares               float64
	RebalancedShares     float64 // New field to hold shares after rebalancing
	CostBasis            float64
	StartMarketValue     float64
	EndMarketValue       float64
	RebalanceMarketValue float64
	RebalanceCash        float64
}

// -----------------------
// getUserInput reads user inputs and builds random allocations.
func getUserInput() (startDate string, endDate string, investment float64) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("\nPortfolio Optimization Setup")
	fmt.Println(strings.Repeat("=", 50))

	// Get tax bracket info
	fmt.Println("\nSelect your income tax bracket:")
	fmt.Println(strings.Repeat("-", 50))
	for k, v := range incomeBrackets {
		fmt.Printf("%d. $%s\n", k, v)
	}
	var taxBracket int
	for {
		fmt.Print("\nEnter the number corresponding to your tax bracket (1-7): ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		t, err := strconv.Atoi(input)
		if err == nil && t >= 1 && t <= 7 {
			taxBracket = t
			break
		}
		fmt.Println("Invalid input. Please enter a number between 1 and 7.")
	}
	USER_TAX_BRACKET_NUM = taxBracket
	USER_TAX_BRACKET = taxBracketMapping[taxBracket]
	fmt.Printf("\nSelected tax bracket: %s (Rate: %.1f%%)\n", incomeBrackets[taxBracket], USER_TAX_BRACKET*100)

	// Get start date
	for {
		fmt.Print("\nEnter start date (YYYY-MM-DD): ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		if _, err := time.Parse("2006-01-02", input); err == nil {
			startDate = input
			break
		}
		fmt.Println("Invalid date format. Please use YYYY-MM-DD format.")
	}

	// Get end date
	for {
		fmt.Print("Enter end date (YYYY-MM-DD): ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		if _, err := time.Parse("2006-01-02", input); err == nil {
			endDate = input
			break
		}
		fmt.Println("Invalid date format. Please use YYYY-MM-DD format.")
	}

	// Get total investment
	for {
		fmt.Print("Enter total portfolio value: $")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		inv, err := strconv.ParseFloat(input, 64)
		if err == nil && inv > 0 {
			investment = inv
			break
		}
		fmt.Println("Invalid number. Please enter a numeric value greater than zero.")
	}
	totalInvestment = investment

	fmt.Println("\nGenerating random portfolio allocations...")

	// Generate random allocations for all tickers
	type TickerCategory struct {
		Category string
		Ticker   string
	}
	var selectedTickers []TickerCategory
	for cat, tks := range categories {
		for _, tk := range tks {
			selectedTickers = append(selectedTickers, TickerCategory{Category: cat, Ticker: tk})
		}
	}

	// Generate random weights and normalize them
	weights := make([]float64, len(selectedTickers))
	var sum float64 = 0
	for i := range weights {
		weights[i] = rand.Float64()
		sum += weights[i]
	}
	// Normalize weights and populate portfolioStructure
	fmt.Println("\nPortfolio Allocation")
	fmt.Println(strings.Repeat("=", 40))
	fmt.Printf("%-15s%-10s%12s\n", "Category", "Ticker", "Weight")
	fmt.Println(strings.Repeat("-", 40))
	for i, tc := range selectedTickers {
		weight := weights[i] / sum
		if portfolioStructure[tc.Category] == nil {
			portfolioStructure[tc.Category] = make(map[string]float64)
		}
		portfolioStructure[tc.Category][tc.Ticker] = weight
		fmt.Printf("%-15s%-10s%11.2f%%\n", tc.Category, tc.Ticker, weight*100)
	}
	fmt.Println(strings.Repeat("-", 40))
	fmt.Printf("%25s%11.2f%%\n", "Total:", 100.0)
	fmt.Println("\nStarting portfolio simulation...")

	return startDate, endDate, investment
}

// -----------------------
// getTickerData simulates fetching pricing data for a ticker by returning randomized prices.
func getTickerData(ticker string, period1, period2 int64) (float64, float64, error) {
	// Generate a random starting price between $50 and $150.
	startPrice := 50 + rand.Float64()*100

	// Simulate an ending price by applying a random percentage change between -10% and +10%
	pctChange := (rand.Float64() - 0.5) * 0.2 // This gives a number in the range [-0.1, +0.1]
	endPrice := startPrice * (1 + pctChange)

	return startPrice, endPrice, nil
}

// -----------------------
// fetchHistoricalPrices loops through all tickers in the portfolio and returns two maps:
// historicalPrices: the price at the start date and marketPrices: the price at the end date.
// Updated so that if a ticker's data is missing, the stock remains (using a fallback price)
// and the cost basis remains unchanged.
func fetchHistoricalPrices(startDate, endDate string) (map[string]float64, map[string]float64, error) {
	fmt.Println("Fetching historical prices, please wait...")
	startTime, err := time.Parse("2006-01-02", startDate)
	if err != nil {
		return nil, nil, err
	}
	endTime, err := time.Parse("2006-01-02", endDate)
	if err != nil {
		return nil, nil, err
	}
	period1 := startTime.Unix()
	period2 := endTime.Unix() + 86400 // add one day in seconds so that end date is included

	historicalPrices := make(map[string]float64)
	marketPrices := make(map[string]float64)

	// Gather all tickers from portfolioStructure.
	var tickers []string
	for _, tickersMap := range portfolioStructure {
		for ticker := range tickersMap {
			tickers = append(tickers, ticker)
		}
	}

	// Instead of removing tickers with missing data,
	// assign a fallback price so that stocks and cost basis remain unchanged.
	for _, ticker := range tickers {
		firstPrice, lastPrice, err := getTickerData(ticker, period1, period2)
		if err != nil {
			fmt.Printf("Warning: No data for ticker %s. Using fallback price of $100.0 instead.\n", ticker)
			historicalPrices[ticker] = 100.0
			marketPrices[ticker] = 100.0
		} else {
			historicalPrices[ticker] = firstPrice
			marketPrices[ticker] = lastPrice
		}
	}

	if len(historicalPrices) == 0 || len(marketPrices) == 0 {
		return nil, nil, errors.New("no valid data available")
	}

	return historicalPrices, marketPrices, nil
}

// -----------------------
// initializePortfolio uses the historical "start" prices to determine shares and cost basis.
func initializePortfolio(historicalPrices map[string]float64) {
	for cat, assets := range portfolioStructure {
		if portfolio[cat] == nil {
			portfolio[cat] = make(map[string]*Position)
		}
		for ticker, weight := range assets {
			maxInvestment := equitiesPercentage * weight * totalInvestment
			costBasis, exists := historicalPrices[ticker]
			if !exists || costBasis == 0 {
				costBasis = 100.0
			}
			shares := maxInvestment / costBasis
			portfolio[cat][ticker] = &Position{
				Shares:           shares,
				RebalancedShares: shares,
				CostBasis:        costBasis,
				StartMarketValue: shares * costBasis,
				EndMarketValue:   0.0,
			}
		}
	}
}

// -----------------------
// calculatePortfolioValue returns the overall value given a price map.
func calculatePortfolioValue(prices map[string]float64) float64 {
	total := 0.0
	for cat, assets := range portfolioStructure {
		for ticker := range assets {
			price, exists := prices[ticker]
			if !exists {
				price = 100.0
			}
			if pos, ok := portfolio[cat][ticker]; ok {
				total += pos.Shares * price
			}
		}
	}
	return total
}

// -----------------------
// rebalance recalculates each asset's shares so that its market value
// equals its original target weight times the current total portfolio value.
func rebalance(prices map[string]float64) {
	totalValue := calculatePortfolioValue(prices)
	// Use the fixed start-date weights in portfolioStructure.
	for cat, tickersMap := range portfolioStructure {
		for ticker, targetWeight := range tickersMap {
			targetValue := targetWeight * totalValue
			currentPrice, exists := prices[ticker]
			if !exists {
				currentPrice = 100.0 // Fallback price
			}
			newShares := targetValue / currentPrice
			if pos, found := portfolio[cat][ticker]; found {
				pos.RebalancedShares = newShares
				pos.RebalanceMarketValue = newShares * currentPrice
			}
		}
	}
}

// -----------------------
// displayPortfolio prints portfolio details. If showRebalance is true the output includes the rebalanced cash flow.
func displayPortfolio(currentPrices map[string]float64, label string, showRebalance bool) {
	totalValue := calculatePortfolioValue(currentPrices)
	fmt.Printf("\n%s: Total Portfolio Value: $%.2f\n", label, totalValue)

	if !strings.HasPrefix(label, "Portfolio at Start") {
		startTotal := 0.0
		for _, assets := range portfolio {
			for _, pos := range assets {
				startTotal += pos.StartMarketValue
			}
		}
		totalPercentChange := ((totalValue - startTotal) / startTotal) * 100
		fmt.Printf("Total Portfolio Change: %.2f%%\n", totalPercentChange)
	}

	if strings.HasPrefix(label, "Portfolio at Start") {
		fmt.Printf("%-15s%-10s%-10s%-15s%15s%10s\n", "Category", "Ticker", "Shares", "Cost Basis", "Market Value", "Weight")
		fmt.Println(strings.Repeat("=", 90))
	} else if showRebalance {
		fmt.Printf("%-15s%-10s%-10s%-15s%15s%15s%15s%20s%10s\n", "Category", "Ticker", "Shares", "Cost Basis", "Current Price", "Market Value", "Dollar Change", "Rebalance Cash Flow", "Weight")
		fmt.Println(strings.Repeat("=", 140))
	} else {
		fmt.Printf("%-15s%-10s%-10s%-15s%15s%15s%10s%15s%10s\n", "Category", "Ticker", "Shares", "Cost Basis", "Current Price", "Market Value", "% Change", "Dollar Change", "Weight")
		fmt.Println(strings.Repeat("=", 130))
	}

	for cat, assets := range portfolioStructure {
		for ticker := range assets {
			pos := portfolio[cat][ticker]
			currPrice, exists := currentPrices[ticker]
			if !exists {
				currPrice = 100.0
			}
			marketValue := pos.Shares * currPrice
			weightPercent := (marketValue / totalValue) * 100
			startValue := pos.StartMarketValue
			if strings.HasPrefix(label, "Portfolio at Start") {
				fmt.Printf("%-15s%-10s%-10.2f%-15.2f%15.2f%9.2f%%\n", cat, ticker, pos.Shares, pos.CostBasis, marketValue, weightPercent)
			} else {
				dollarChange := marketValue - startValue
				percentChange := 0.0
				if startValue > 0 {
					percentChange = ((marketValue - startValue) / startValue) * 100
				}
				if showRebalance {
					// Here, we compare the new market value with the previous end-market value.
					endValue := pos.EndMarketValue
					rebalanceFlow := marketValue - endValue
					fmt.Printf("%-15s%-10s%-10.2f%-15.2f%15.2f%15.2f%15.2f%20.2f%9.2f%%\n", cat, ticker, pos.RebalancedShares, pos.CostBasis, currPrice, marketValue, dollarChange, rebalanceFlow, weightPercent)
				} else {
					pos.EndMarketValue = marketValue
					fmt.Printf("%-15s%-10s%-10.2f%-15.2f%15.2f%15.2f%9.2f%%%15.2f%9.2f%%\n", cat, ticker, pos.Shares, pos.CostBasis, currPrice, marketValue, percentChange, dollarChange, weightPercent)
				}
			}
		}
	}
}

// -----------------------
// Helper functions to compute statistics.
func mean(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range data {
		sum += v
	}
	return sum / float64(len(data))
}

func stdDev(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	m := mean(data)
	sumSq := 0.0
	for _, v := range data {
		sumSq += (v - m) * (v - m)
	}
	return math.Sqrt(sumSq / float64(len(data)))
}

func percentile(data []float64, percent float64) float64 {
	if len(data) == 0 {
		return 0
	}
	sorted := make([]float64, len(data))
	copy(sorted, data)
	sort.Float64s(sorted)
	index := (percent / 100.0) * float64(len(sorted)-1)
	lower := int(math.Floor(index))
	upper := int(math.Ceil(index))
	if lower == upper {
		return sorted[lower]
	}
	weight := index - float64(lower)
	return sorted[lower]*(1-weight) + sorted[upper]*weight
}

// -----------------------
// analyzePerformance computes overall statistics and identifies outperformers/underperformers.
func analyzePerformance(historicalPrices, marketPrices map[string]float64) {
	var percentChanges []float64
	type TickerChange struct {
		Ticker   string
		Category string
		Change   float64
	}
	var tickerChanges []TickerChange

	for cat, assets := range portfolioStructure {
		for ticker := range assets {
			pos := portfolio[cat][ticker]
			startValue := pos.StartMarketValue
			endValue := pos.EndMarketValue
			if startValue == 0 {
				continue
			}
			percentChange := ((endValue - startValue) / startValue) * 100
			percentChanges = append(percentChanges, percentChange)
			tickerChanges = append(tickerChanges, TickerChange{Ticker: ticker, Category: cat, Change: percentChange})
		}
	}

	meanChange := mean(percentChanges)
	sd := stdDev(percentChanges)
	p90 := percentile(percentChanges, 90)
	p10 := percentile(percentChanges, 10)

	outperformerThreshold := meanChange + sd       // mean+1*stdDev
	underperformerThreshold := meanChange - 0.5*sd // mean-0.5*stdDev

	var stdOutperformers, stdUnderperformers []TickerChange
	var percentileOutperformers, percentileUnderperformers []TickerChange

	for _, tc := range tickerChanges {
		if tc.Change > outperformerThreshold {
			stdOutperformers = append(stdOutperformers, tc)
		} else if tc.Change < underperformerThreshold {
			stdUnderperformers = append(stdUnderperformers, tc)
		}

		if tc.Change >= p90 {
			percentileOutperformers = append(percentileOutperformers, tc)
		} else if tc.Change <= p10 {
			percentileUnderperformers = append(percentileUnderperformers, tc)
		}
	}

	fmt.Println("\nPerformance Analysis Report")
	fmt.Println(strings.Repeat("=", 50))
	fmt.Println("\nOverall Statistics:")
	fmt.Println(strings.Repeat("-", 50))
	fmt.Printf("%-20s%15.2f%%\n", "Mean Return:", meanChange)
	fmt.Printf("%-20s%15.2f%%\n", "Std Deviation:", sd)
	fmt.Println(strings.Repeat("-", 50))
	fmt.Printf("%-20s%15.2f%%\n", "90th Percentile:", p90)
	fmt.Printf("%-20s%15.2f%%\n", "10th Percentile:", p10)
	fmt.Println(strings.Repeat("-", 50))

	fmt.Println("\nStandard Deviation Analysis")
	fmt.Printf("Stocks with returns above %.2f%% (mean + 1.0 std) are outperformers\n", outperformerThreshold)
	fmt.Printf("Stocks with returns below %.2f%% (mean - 0.5 std) are underperformers\n", underperformerThreshold)
	fmt.Println(strings.Repeat("-", 50))
	fmt.Printf("%-10s%-15s%10s\n", "Ticker", "Category", "Return %")

	if len(stdOutperformers) > 0 {
		fmt.Println("\nOutperformers:")
		fmt.Println(strings.Repeat("-", 35))
		sort.Slice(stdOutperformers, func(i, j int) bool {
			return stdOutperformers[i].Change > stdOutperformers[j].Change
		})
		for _, tc := range stdOutperformers {
			fmt.Printf("%-10s%-15s%10.2f%%\n", tc.Ticker, tc.Category, tc.Change)
		}
	} else {
		fmt.Println("\nNo outperformers based on standard deviation")
	}

	if len(stdUnderperformers) > 0 {
		fmt.Println("\nUnderperformers:")
		fmt.Println(strings.Repeat("-", 35))
		sort.Slice(stdUnderperformers, func(i, j int) bool {
			return stdUnderperformers[i].Change < stdUnderperformers[j].Change
		})
		for _, tc := range stdUnderperformers {
			fmt.Printf("%-10s%-15s%10.2f%%\n", tc.Ticker, tc.Category, tc.Change)
		}
	} else {
		fmt.Println("\nNo underperformers based on standard deviation")
	}
}

// -----------------------
// bracketLowerBound returns the lower bound of the tax bracket based on the user's selection.
func bracketLowerBound(bracketNum int) float64 {
	switch bracketNum {
	case 1:
		return 0
	case 2:
		return 11000
	case 3:
		return 44725
	case 4:
		return 95375
	case 5:
		return 182100
	case 6:
		return 231250
	case 7:
		return 578125
	default:
		return 0
	}
}

// TaxBracket is used for progressive tax calculations.
type TaxBracket struct {
	Lower float64
	Upper float64
	Rate  float64
}

// computeTax calculates tax on an income amount given progressive brackets.
func computeTax(income float64, brackets []TaxBracket) float64 {
	tax := 0.0
	for _, b := range brackets {
		if income > b.Lower {
			// Taxable amount in this bracket is the lesser of (income - lower bound) and bracket width.
			taxable := math.Min(income, b.Upper) - b.Lower
			tax += taxable * b.Rate
		}
	}
	return tax
}

// calculateTaxImplications computes the tax liability (or benefit) based on realized gains/losses
// from positions sold during rebalancing. It now looks at the current bracket by adding in the
// net realized gain. For short-term, it applies the user's selected tax rate, while for long-term,
// it uses a simple scheme: taxable income up to $48,350 pays 0%, up to $533,400 pays 15%,
// and anything above pays 20%.
func calculateTaxImplications(historicalPrices, marketPrices map[string]float64, holdingPeriodDays int) float64 {
	fmt.Println("\nTax Implications Analysis")
	fmt.Println(strings.Repeat("=", 50))
	isLongTerm := holdingPeriodDays > 365
	termLabel := "Short-Term"
	if isLongTerm {
		termLabel = "Long-Term"
	}
	fmt.Printf("Holding Period: %d days (%s)\n", holdingPeriodDays, termLabel)

	var totalRealizedGain, totalRealizedLoss float64

	for cat := range portfolioStructure {
		for ticker, pos := range portfolio[cat] {
			// Realized gain/loss is calculated from the difference between the ending market value
			// and the value after rebalancing.
			saleProceeds := pos.EndMarketValue - pos.RebalanceMarketValue
			if saleProceeds > 0 {
				currentPrice, exists := marketPrices[ticker]
				if !exists || currentPrice <= 0 {
					currentPrice = 100.0
				}
				startPrice, found := historicalPrices[ticker]
				if !found || startPrice <= 0 {
					startPrice = pos.CostBasis
				}
				sharesSold := saleProceeds / currentPrice
				realized := sharesSold * (currentPrice - startPrice)
				if realized >= 0 {
					totalRealizedGain += realized
				} else {
					totalRealizedLoss += -realized
				}
			}
		}
	}

	netRealized := totalRealizedGain - totalRealizedLoss

	fmt.Println(strings.Repeat("-", 65))
	fmt.Printf("%-25s: $%.2f\n", "Total Realized Gains", totalRealizedGain)
	fmt.Printf("%-25s: $%.2f\n", "Total Realized Losses", totalRealizedLoss)
	fmt.Printf("%-25s: $%.2f\n", "Net Realized Gain", netRealized)

	var incrementalTax float64
	if netRealized != 0 {
		// The base income is determined by the lower bound of the user's selected bracket.
		baseIncome := bracketLowerBound(USER_TAX_BRACKET_NUM)
		newIncome := baseIncome + netRealized
		var effectiveRate float64
		if isLongTerm {
			// Simple long-term brackets.
			if newIncome <= 48350 {
				effectiveRate = 0.0
			} else if newIncome <= 533400 {
				effectiveRate = 0.15
			} else {
				effectiveRate = 0.20
			}
			incrementalTax = netRealized * effectiveRate
			fmt.Printf("Long-term capital gains applied (rate after: %.2f%%)\n", effectiveRate*100)
		} else {
			// Short-term gains are taxed at the ordinary income rate selected by the user.
			effectiveRate = USER_TAX_BRACKET
			incrementalTax = netRealized * effectiveRate
			fmt.Printf("Short-term capital gains applied (rate after: %.2f%%)\n", effectiveRate*100)
		}
	}

	fmt.Println(strings.Repeat("-", 65))
	if netRealized >= 0 {
		fmt.Printf("%-25s: $%.2f\n", "Estimated Tax Liability", incrementalTax)
	} else {
		fmt.Printf("%-25s: $%.2f\n", "Tax Write-Off Benefit", -incrementalTax)
	}

	return incrementalTax
}

// -----------------------
// main executes the simulation.
func main() {
	rand.Seed(time.Now().UnixNano())

	startDate, endDate, _ := getUserInput()

	historicalPrices, marketPrices, err := fetchHistoricalPrices(startDate, endDate)
	if err != nil {
		fmt.Println("\nCannot proceed with simulation due to data issues:", err)
		os.Exit(1)
	}

	initializePortfolio(historicalPrices)
	// Display initial portfolio with start prices
	displayPortfolio(historicalPrices, fmt.Sprintf("Portfolio at Start Date (%s)", startDate), false)
	// Display portfolio using end date prices
	displayPortfolio(marketPrices, fmt.Sprintf("Portfolio at End Date (%s)", endDate), false)

	fmt.Println("\nRebalancing Portfolio...")
	rebalance(marketPrices)
	displayPortfolio(marketPrices, "After Rebalancing (End-Date Prices)", true)

	analyzePerformance(historicalPrices, marketPrices)

	tStart, _ := time.Parse("2006-01-02", startDate)
	tEnd, _ := time.Parse("2006-01-02", endDate)
	holdingPeriodDays := int(tEnd.Sub(tStart).Hours() / 24)

	_ = calculateTaxImplications(historicalPrices, marketPrices, holdingPeriodDays)
}
