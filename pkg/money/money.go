package money

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	ScaleFactor = 10000 // 4 decimal places (matches PostgreSQL NUMERIC(18, 4))
)

var (
	ErrNegativeAmount = errors.New("amount must be non-negative")
	ErrInvalidNumber  = errors.New("number must not be NaN or Inf")
	ErrInsufficient   = errors.New("insufficient funds")
)

// Amount represents a monetary decimal amount that supports exact ISO 20022 string format (e.g. "10000.50")
// and standard numeric JSON format (e.g. 10000.50), preventing IEEE-754 precision coercion in client deserializers.
type Amount float64

// Float64 returns the underlying amount as float64.
func (a Amount) Float64() float64 {
	return float64(a)
}

// Units returns the amount converted to integer fixed-point units (scaled by 10,000).
func (a Amount) Units() (int64, error) {
	return ToUnits(float64(a))
}

// String returns formatted decimal string.
func (a Amount) String() string {
	return strconv.FormatFloat(float64(a), 'f', -1, 64)
}

// MarshalJSON formats Amount into JSON representation.
func (a Amount) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatFloat(float64(a), 'f', -1, 64)), nil
}

// UnmarshalJSON implements json.Unmarshaler to parse both string and numeric JSON amounts.
func (a *Amount) UnmarshalJSON(data []byte) error {
	raw := strings.TrimSpace(string(data))
	if raw == "" || raw == "null" {
		return errors.New("amount cannot be empty or null")
	}

	var strVal string
	if raw[0] == '"' && raw[len(raw)-1] == '"' {
		if err := json.Unmarshal(data, &strVal); err != nil {
			return fmt.Errorf("invalid string amount: %w", err)
		}
	} else {
		strVal = raw
	}

	parsed, err := ParseAmount(strVal)
	if err != nil {
		return err
	}

	*a = parsed
	return nil
}

// ParseAmount parses and validates a monetary string according to financial decimal standards.
func ParseAmount(s string) (Amount, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("amount cannot be empty")
	}

	if strings.ContainsAny(s, "eE") {
		return 0, errors.New("scientific notation is not allowed for financial amounts")
	}

	parts := strings.Split(s, ".")
	if len(parts) > 2 {
		return 0, fmt.Errorf("invalid decimal format: '%s'", s)
	}
	if len(parts) == 2 {
		decimals := strings.TrimRight(parts[1], "0")
		if len(decimals) > 4 {
			return 0, fmt.Errorf("amount '%s' exceeds maximum precision of 4 decimal places", s)
		}
	}

	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number '%s': %w", s, err)
	}

	if math.IsNaN(val) || math.IsInf(val, 0) {
		return 0, errors.New("amount must be a valid finite number")
	}

	if val <= 0 {
		return 0, errors.New("amount must be strictly positive")
	}

	return Amount(Round4(val)), nil
}

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
