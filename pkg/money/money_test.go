package money_test

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/wwafii/distributed-financial-ledger/pkg/money"
)

func TestMoney_FixedPointPrecision(t *testing.T) {
	// IEEE-754 classic flaw: 0.1 + 0.2 != 0.3 in raw binary floating-point
	val1 := 0.1
	val2 := 0.2
	rawSum := val1 + val2
	if rawSum == 0.3 {
		t.Log("rawSum happened to equal 0.3, checking exact bit representation")
	}

	exactSum, err := money.Add(val1, val2)
	if err != nil {
		t.Fatalf("unexpected error on Add: %v", err)
	}
	if exactSum != 0.3 {
		t.Errorf("expected exactly 0.3, got %v", exactSum)
	}

	// Repeated additions: 0.1 added 10 times must be exactly 1.0
	var sum float64 = 0.0
	for i := 0; i < 10; i++ {
		sum, err = money.Add(sum, 0.1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if sum != 1.0 {
		t.Errorf("expected 1.0, got %v", sum)
	}
}

func TestMoney_Sub_Insufficient(t *testing.T) {
	balance := 100.0000
	amount := 100.0001

	_, err := money.Sub(balance, amount)
	if err != money.ErrInsufficient {
		t.Errorf("expected ErrInsufficient, got: %v", err)
	}
}

func TestMoney_InvalidNumbers(t *testing.T) {
	if _, err := money.ToUnits(math.NaN()); err != money.ErrInvalidNumber {
		t.Errorf("expected ErrInvalidNumber for NaN")
	}
	if _, err := money.ToUnits(math.Inf(1)); err != money.ErrInvalidNumber {
		t.Errorf("expected ErrInvalidNumber for +Inf")
	}
	if _, err := money.ToUnits(-10.5); err != money.ErrNegativeAmount {
		t.Errorf("expected ErrNegativeAmount for negative input")
	}
}

func TestMoney_AmountJSON_StringAndNumber(t *testing.T) {
	type TestPayload struct {
		Amount money.Amount `json:"amount"`
	}

	// 1. ISO 20022 String format
	rawJSONString := []byte(`{"amount": "10000.50"}`)
	var p1 TestPayload
	if err := json.Unmarshal(rawJSONString, &p1); err != nil {
		t.Fatalf("failed to unmarshal string amount: %v", err)
	}
	if p1.Amount.Float64() != 10000.50 {
		t.Errorf("expected 10000.50, got %v", p1.Amount.Float64())
	}
	units, err := p1.Amount.Units()
	if err != nil || units != 100005000 {
		t.Errorf("expected 100005000 units, got %d, err %v", units, err)
	}

	// 2. Numeric format (backward compatible)
	rawJSONNum := []byte(`{"amount": 25000.0}`)
	var p2 TestPayload
	if err := json.Unmarshal(rawJSONNum, &p2); err != nil {
		t.Fatalf("failed to unmarshal numeric amount: %v", err)
	}
	if p2.Amount.Float64() != 25000.0 {
		t.Errorf("expected 25000.0, got %v", p2.Amount.Float64())
	}

	// 3. String format with 4 decimal places
	rawJSON4Dec := []byte(`{"amount": "123.4567"}`)
	var p3 TestPayload
	if err := json.Unmarshal(rawJSON4Dec, &p3); err != nil {
		t.Fatalf("failed to unmarshal 4-decimal amount: %v", err)
	}
	if p3.Amount.Float64() != 123.4567 {
		t.Errorf("expected 123.4567, got %v", p3.Amount.Float64())
	}

	// 4. Excessive precision (> 4 decimals) must fail
	rawJSON5Dec := []byte(`{"amount": "123.45678"}`)
	var p4 TestPayload
	if err := json.Unmarshal(rawJSON5Dec, &p4); err == nil {
		t.Errorf("expected error for >4 decimal places, got nil")
	}

	// 5. Scientific notation must fail
	rawJSONSci := []byte(`{"amount": "1e5"}`)
	var p5 TestPayload
	if err := json.Unmarshal(rawJSONSci, &p5); err == nil {
		t.Errorf("expected error for scientific notation, got nil")
	}

	// 6. Negative amount must fail
	rawJSONNeg := []byte(`{"amount": "-500"}`)
	var p6 TestPayload
	if err := json.Unmarshal(rawJSONNeg, &p6); err == nil {
		t.Errorf("expected error for negative amount, got nil")
	}

	// 7. Zero amount must fail
	rawJSONZero := []byte(`{"amount": "0"}`)
	var p7 TestPayload
	if err := json.Unmarshal(rawJSONZero, &p7); err == nil {
		t.Errorf("expected error for zero amount, got nil")
	}
}
