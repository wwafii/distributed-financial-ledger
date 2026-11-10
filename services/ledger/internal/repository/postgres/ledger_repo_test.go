package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/wwafii/distributed-financial-ledger/pkg/errs"
	"github.com/wwafii/distributed-financial-ledger/services/ledger/internal/domain"
	"github.com/wwafii/distributed-financial-ledger/services/ledger/internal/repository/postgres"
)

const testDBURL = "postgres://postgres:postgres@localhost:5433/ledger_db?sslmode=disable"

func setupTestPool(t *testing.T) *pgxpool.Pool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, testDBURL)
	if err != nil {
		t.Skipf("Skipping integration test; PostgreSQL not accessible: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("Skipping integration test; PostgreSQL ping failed: %v", err)
	}
	return pool
}

func TestExecuteTransfer_DoubleEntrySuccess(t *testing.T) {
	pool := setupTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	repo := postgres.NewLedgerRepository(pool)

	refID := "test-ref-" + uuid.New().String()
	cmd := &domain.TransferCommand{
		ReferenceID:    refID,
		SourceAccount:  "ACC-ALICE-01",
		DestinationAcc: "ACC-BOB-02",
		Amount:         100000.0,
		Currency:       "IDR",
		Description:    "Test transfer Alice to Bob",
	}

	aliceBefore, err := repo.GetAccountByNumber(ctx, "ACC-ALICE-01")
	if err != nil {
		t.Fatalf("failed to fetch alice before: %v", err)
	}
	bobBefore, err := repo.GetAccountByNumber(ctx, "ACC-BOB-02")
	if err != nil {
		t.Fatalf("failed to fetch bob before: %v", err)
	}

	txResult, err := repo.ExecuteTransfer(ctx, cmd)
	if err != nil {
		t.Fatalf("ExecuteTransfer failed: %v", err)
	}

	if txResult.ReferenceID != refID {
		t.Errorf("expected refID %s, got %s", refID, txResult.ReferenceID)
	}

	aliceAfter, err := repo.GetAccountByNumber(ctx, "ACC-ALICE-01")
	if err != nil {
		t.Fatalf("failed to fetch alice after: %v", err)
	}
	bobAfter, err := repo.GetAccountByNumber(ctx, "ACC-BOB-02")
	if err != nil {
		t.Fatalf("failed to fetch bob after: %v", err)
	}

	expectedAlice := aliceBefore.Balance - 100000.0
	expectedBob := bobBefore.Balance + 100000.0

	if aliceAfter.Balance != expectedAlice {
		t.Errorf("Alice balance mismatch: expected %.2f, got %.2f", expectedAlice, aliceAfter.Balance)
	}
	if bobAfter.Balance != expectedBob {
		t.Errorf("Bob balance mismatch: expected %.2f, got %.2f", expectedBob, bobAfter.Balance)
	}

	// Test Idempotency with exact same reference_id
	repeatTx, err := repo.ExecuteTransfer(ctx, cmd)
	if err != nil {
		t.Fatalf("Repeat ExecuteTransfer should succeed idempotently, got: %v", err)
	}
	if repeatTx.ID != txResult.ID {
		t.Errorf("Expected same transaction ID %s, got %s", txResult.ID, repeatTx.ID)
	}

	// Verify balance was NOT deducted a second time
	aliceAfterRepeat, _ := repo.GetAccountByNumber(ctx, "ACC-ALICE-01")
	if aliceAfterRepeat.Balance != expectedAlice {
		t.Errorf("Alice balance changed after repeated call: %.2f", aliceAfterRepeat.Balance)
	}
}

func TestExecuteTransfer_InsufficientFunds(t *testing.T) {
	pool := setupTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	repo := postgres.NewLedgerRepository(pool)

	cmd := &domain.TransferCommand{
		ReferenceID:    "test-fail-" + uuid.New().String(),
		SourceAccount:  "ACC-BOB-02",
		DestinationAcc: "ACC-ALICE-01",
		Amount:         999999999.0, // Exceeds balance
		Currency:       "IDR",
		Description:    "Test excessive transfer",
	}

	_, err := repo.ExecuteTransfer(ctx, cmd)
	if err == nil {
		t.Fatalf("expected error due to insufficient balance, got nil")
	}

	if !errs.Is(err, errs.CodeInsufficient) {
		t.Errorf("expected error code INSUFFICIENT_FUNDS, got: %v", err)
	}
}
