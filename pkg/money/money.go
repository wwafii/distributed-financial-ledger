package money

import (
	"errors"
	"fmt"
	"math"
)

const (
	ScaleFactor = 10000 // 4 decimal places (matches PostgreSQL NUMERIC(18, 4))
)

var (
	ErrNegativeAmount = errors.New("amount must be non-negative")
	ErrInvalidNumber  = errors.New("number must not be NaN or Inf")
	ErrInsufficient   = errors.New("insufficient funds")
)

// ToUnits converts a float64 amount into fixed-point integer units (scaled by 10,000)
func ToUnits(amount float64) (int64, error) {
	if math.IsNaN(amount) || math.IsInf(amount, 0) {
		return 0, ErrInvalidNumber
	}
	if amount < 0 {
		return 0, ErrNegativeAmount
	}
	// Round half away from zero to prevent IEEE-754 binary floating representation error
	return int64(math.Round(amount * ScaleFactor)), nil
}

// FromUnits converts fixed-point integer units back to float64 scaled by 10,000
func FromUnits(units int64) float64 {
	return float64(units) / ScaleFactor
}

// Round4 returns a float64 strictly rounded to 4 decimal places
func Round4(val float64) float64 {
	if math.IsNaN(val) || math.IsInf(val, 0) {
		return 0
	}
	return math.Round(val*ScaleFactor) / ScaleFactor
}

// Sub subtracts amount from balance using exact fixed-point units, guarding against precision loss
func Sub(balance, amount float64) (float64, error) {
	balUnits, err := ToUnits(balance)
	if err != nil {
		return 0, fmt.Errorf("invalid balance: %w", err)
	}
	amtUnits, err := ToUnits(amount)
	if err != nil {
		return 0, fmt.Errorf("invalid amount: %w", err)
	}

	if balUnits < amtUnits {
		return 0, ErrInsufficient
	}

	return FromUnits(balUnits - amtUnits), nil
}

// Add adds amount to balance using exact fixed-point units, guarding against precision loss
func Add(balance, amount float64) (float64, error) {
	balUnits, err := ToUnits(balance)
	if err != nil {
		return 0, fmt.Errorf("invalid balance: %w", err)
	}
	amtUnits, err := ToUnits(amount)
	if err != nil {
		return 0, fmt.Errorf("invalid amount: %w", err)
	}

	return FromUnits(balUnits + amtUnits), nil
}
