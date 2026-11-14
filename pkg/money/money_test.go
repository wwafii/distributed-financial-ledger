package money_test

import (
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
