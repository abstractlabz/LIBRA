package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
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

// Position represents an asset holding.
type Position struct {
	Shares               float64
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
// getTickerData fetches CSV data from Yahoo Finance and extracts the first and last "Close" values.
func getTickerData(ticker string, period1, period2 int64) (float64, float64, error) {
	url := fmt.Sprintf(
		"https://query1.finance.yahoo.com/v7/finance/download/%s?period1=%d&period2=%d&interval=1d&events=history",
		ticker, period1, period2,
	)

	// Create a new HTTP request so we can set custom headers
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, 0, err
	}
	// Set a User-Agent header to mimic a browser
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return 0, 0, fmt.Errorf("HTTP error: %s", resp.Status)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, err
	}

	r := csv.NewReader(strings.NewReader(string(body)))
	records, err := r.ReadAll()
	if err != nil {
		return 0, 0, err
	}
	if len(records) < 2 {
		return 0, 0, errors.New("no data in CSV")
	}

	// Find the "Close" column index in the header (defaulting to index 4)
	header := records[0]
	closeIndex := -1
	for i, col := range header {
		if col == "Close" {
			closeIndex = i
			break
		}
	}
	if closeIndex == -1 {
		closeIndex = 4
	}
	// First data row and last data row
	firstRow := records[1]
	lastRow := records[len(records)-1]
	firstPrice, err := strconv.ParseFloat(firstRow[closeIndex], 64)
	if err != nil {
		return 0, 0, err
	}
	lastPrice, err := strconv.ParseFloat(lastRow[closeIndex], 64)
	if err != nil {
		return 0, 0, err
	}
	return firstPrice, lastPrice, nil
}

// -----------------------
// fetchHistoricalPrices loops through all tickers in the portfolio and returns two maps:
// historicalPrices: the price at the start date and marketPrices: the price at the end date.
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

	missingTickers := []string{}
	for _, ticker := range tickers {
		firstPrice, lastPrice, err := getTickerData(ticker, period1, period2)
		if err != nil {
			fmt.Printf("Warning: No data for ticker %s. Error: %s\n", ticker, err.Error())
			missingTickers = append(missingTickers, ticker)
			continue
		}
		historicalPrices[ticker] = firstPrice
		marketPrices[ticker] = lastPrice
	}

	// Remove missing tickers from portfolioStructure and rebalance weights.
	if len(missingTickers) > 0 {
		fmt.Println("\nWarning: No data available for the following tickers:")
		for _, t := range missingTickers {
			fmt.Printf("- %s\n", t)
		}
		for cat, tkMap := range portfolioStructure {
			for _, mt := range missingTickers {
				if _, exists := tkMap[mt]; exists {
					delete(tkMap, mt)
				}
			}
			if len(tkMap) == 0 {
				delete(portfolioStructure, cat)
			}
		}
		// Rebalance weights
		totalWeight := 0.0
		for _, tkMap := range portfolioStructure {
			for _, w := range tkMap {
				totalWeight += w
			}
		}
		for cat, tkMap := range portfolioStructure {
			for tk, w := range tkMap {
				portfolioStructure[cat][tk] = w / totalWeight
			}
		}
		fmt.Println("\nPortfolio weights have been automatically rebalanced among available tickers:")
		fmt.Printf("%-15s%-10s%12s\n", "Category", "Ticker", "New Weight")
		fmt.Println(strings.Repeat("-", 37))
		for cat, tkMap := range portfolioStructure {
			for tk, w := range tkMap {
				fmt.Printf("%-15s%-10s%11.2f%%\n", cat, tk, w*100)
			}
		}
		fmt.Println(strings.Repeat("-", 37))
	}
	if len(historicalPrices) == 0 || len(marketPrices) == 0 {
		return nil, nil, errors.New("no valid data remaining after removing problematic tickers")
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
// rebalance adjusts the portfolio positions so that each asset is allocated its target weight.
func rebalance(prices map[string]float64) {
	totalValue := calculatePortfolioValue(prices)
	for cat, assets := range portfolioStructure {
		for ticker, weight := range assets {
			targetValue := weight * totalValue
			currPrice, exists := prices[ticker]
			if !exists {
				currPrice = 100.0
			}
			newShares := targetValue / currPrice
			if pos, ok := portfolio[cat][ticker]; ok {
				pos.Shares = newShares
				pos.RebalanceMarketValue = newShares * currPrice
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
					fmt.Printf("%-15s%-10s%-10.2f%-15.2f%15.2f%15.2f%15.2f%20.2f%9.2f%%\n", cat, ticker, pos.Shares, pos.CostBasis, currPrice, marketValue, dollarChange, rebalanceFlow, weightPercent)
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
// calculateTaxImplications computes gains/losses tax, prints a summary, and returns the total estimated tax.
func calculateTaxImplications(historicalPrices, marketPrices map[string]float64, holdingPeriodDays int) float64 {
	totalGains := 0.0
	totalLosses := 0.0
	exitedCashSum := 0.0 // For positions with negative rebalance cash (not computed here, so remains 0)
	isLongTerm := holdingPeriodDays > 365

	fmt.Println("\nTax Implications Analysis")
	fmt.Println(strings.Repeat("=", 50))
	fmt.Printf("Holding Period: %d days (%s-Term)\n", holdingPeriodDays, map[bool]string{true: "Long", false: "Short"}[isLongTerm])

	totalTax := 0.0
	fmt.Printf("\n%-15s%-10s%15s%10s%15s\n", "Category", "Ticker", "Gain/Loss", "Tax Rate", "Position Tax")
	for cat, assets := range portfolioStructure {
		for ticker := range assets {
			pos := portfolio[cat][ticker]
			startValue := pos.StartMarketValue
			endValue := pos.EndMarketValue
			gainLoss := endValue - startValue
			if gainLoss > 0 {
				totalGains += gainLoss
			} else {
				totalLosses += math.Abs(gainLoss)
			}
			// We'll calculate a position's tax only if there is a gain.
			var positionTax float64
			if gainLoss > 0 {
				positionTax = gainLoss * 0 // provisional: actual tax rate determined below
			}
			_ = positionTax // temporary; we sum tax later
		}
	}

	netGainLoss := totalGains - totalLosses
	taxRate := 0.0
	if netGainLoss > 0 {
		if isLongTerm {
			if netGainLoss <= 44625 {
				taxRate = 0.0
			} else if netGainLoss <= 492300 {
				taxRate = 0.15
			} else {
				taxRate = 0.20
			}
			fmt.Printf("Using long-term capital gains rate: %.1f%%\n", taxRate*100)
		} else {
			taxRate = USER_TAX_BRACKET
			fmt.Printf("Using your tax bracket rate: %.1f%%\n", taxRate*100)
		}
	}

	fmt.Println(strings.Repeat("-", 65))
	for cat, assets := range portfolioStructure {
		for ticker := range assets {
			pos := portfolio[cat][ticker]
			gainLoss := pos.EndMarketValue - pos.StartMarketValue
			positionTax := 0.0
			if gainLoss > 0 {
				positionTax = gainLoss * taxRate
			}
			totalTax += positionTax
			fmt.Printf("%-15s%-10s%15.2f%10.1f%%%15.2f\n", cat, ticker, gainLoss, taxRate*100, positionTax)
		}
	}

	fmt.Println(strings.Repeat("-", 65))
	fmt.Println("\nSummary:")
	fmt.Printf("Total Gains: $%.2f\n", totalGains)
	fmt.Printf("Total Losses: $%.2f\n", totalLosses)
	fmt.Printf("Net Gain/Loss: $%.2f\n", netGainLoss)
	fmt.Printf("Applicable Tax Rate: %.1f%%\n", taxRate*100)
	fmt.Printf("Estimated Tax Liability: $%.2f\n", totalTax)
	fmt.Printf("After-Tax Return: $%.2f\n", netGainLoss-totalTax)

	// Exited positions (if any) tax implications:
	fmt.Println("\nExited Positions Tax Implications:")
	fmt.Println(strings.Repeat("=", 50))
	fmt.Printf("Exited Cash Sum: $%.2f\n", exitedCashSum)
	if exitedCashSum > 0 {
		exitedTax := exitedCashSum * taxRate
		fmt.Printf("Exited Tax Liability: $%.2f\n", exitedTax)
		effectiveTaxRate := exitedTax / exitedCashSum
		fmt.Printf("Effective Tax Rate on Exited Positions: %.1f%%\n", effectiveTaxRate*100)
	} else {
		fmt.Println("No exited cash to tax.")
		fmt.Printf("Effective Tax Rate on Exited Positions: 0.0%%\n")
	}

	return totalTax
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
