package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/wwafii/distributed-financial-ledger/pkg/errs"
	"github.com/wwafii/distributed-financial-ledger/pkg/money"
	"github.com/wwafii/distributed-financial-ledger/pkg/telemetry"
	"github.com/wwafii/distributed-financial-ledger/services/ledger/internal/domain"
)

type LedgerRepository struct {
	pool *pgxpool.Pool
}

func NewLedgerRepository(pool *pgxpool.Pool) *LedgerRepository {
	return &LedgerRepository{pool: pool}
}

// GetAccountByNumber retrieves account details by account number
func (r *LedgerRepository) GetAccountByNumber(ctx context.Context, accNum string) (*domain.Account, error) {
	query := `
		SELECT id, account_number, holder_name, balance, currency, status, created_at, updated_at
		FROM accounts
		WHERE account_number = $1
	`
	var acc domain.Account
	err := r.pool.QueryRow(ctx, query, accNum).Scan(
		&acc.ID,
		&acc.AccountNumber,
		&acc.HolderName,
		&acc.Balance,
		&acc.Currency,
		&acc.Status,
		&acc.CreatedAt,
		&acc.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errs.New(errs.CodeNotFound, fmt.Sprintf("account %s not found", accNum))
		}
		return nil, errs.Wrap(errs.CodeInternal, "failed to query account", err)
	}
	return &acc, nil
}

// FreezeAccount updates account status to FROZEN idempotently
func (r *LedgerRepository) FreezeAccount(ctx context.Context, accNum, reason string) error {
	var currentStatus string
	err := r.pool.QueryRow(ctx, `SELECT status FROM accounts WHERE account_number = $1`, accNum).Scan(&currentStatus)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errs.New(errs.CodeNotFound, fmt.Sprintf("account %s not found", accNum))
		}
		return errs.Wrap(errs.CodeInternal, "failed to query account status", err)
	}

	if currentStatus == string(domain.AccountStatusFrozen) {
		// Idempotent: already frozen, return success cleanly
		return nil
	}

	query := `
		UPDATE accounts 
		SET status = 'FROZEN', updated_at = NOW()
		WHERE account_number = $1 AND status != 'FROZEN'
	`
	_, err = r.pool.Exec(ctx, query, accNum)
	if err != nil {
		return errs.Wrap(errs.CodeInternal, "failed to freeze account", err)
	}
	return nil
}

// ExecuteTransfer performs atomic double-entry transfer with row-level locking
func (r *LedgerRepository) ExecuteTransfer(ctx context.Context, cmd *domain.TransferCommand) (*domain.Transaction, error) {
	if math.IsNaN(cmd.Amount) || math.IsInf(cmd.Amount, 0) || cmd.Amount <= 0 {
		return nil, errs.New(errs.CodeInvalidInput, "transfer amount must be a valid strictly positive number")
	}
	if cmd.SourceAccount == cmd.DestinationAcc {
		return nil, errs.New(errs.CodeInvalidInput, "cannot transfer to the same account")
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, errs.Wrap(errs.CodeInternal, "failed to begin transaction", err)
	}
	defer tx.Rollback(ctx)

	// 1. Check idempotency: Has this reference_id already been posted?
	var existingTxID uuid.UUID
	var existingStatus string
	var existingCreatedAt time.Time
	err = tx.QueryRow(ctx, `
		SELECT id, status, created_at FROM transactions WHERE reference_id = $1
	`, cmd.ReferenceID).Scan(&existingTxID, &existingStatus, &existingCreatedAt)
	if err == nil {
		// Transaction already exists! Return existing record idempotently
		return &domain.Transaction{
			ID:          existingTxID,
			ReferenceID: cmd.ReferenceID,
			Description: cmd.Description,
			Status:      domain.TransactionStatus(existingStatus),
			CreatedAt:   existingCreatedAt,
		}, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, errs.Wrap(errs.CodeInternal, "failed to check existing reference_id", err)
	}

	// 2. Deadlock Prevention Strategy:
	// Always acquire row locks in deterministic (lexicographical) order!
	accList := []string{cmd.SourceAccount, cmd.DestinationAcc}
	sort.Strings(accList)

	accountsMap := make(map[string]*domain.Account, 2)
	for _, accNum := range accList {
		var acc domain.Account
		err := tx.QueryRow(ctx, `
			SELECT id, account_number, holder_name, balance, currency, status, created_at, updated_at
			FROM accounts
			WHERE account_number = $1
			FOR UPDATE
		`, accNum).Scan(
			&acc.ID,
			&acc.AccountNumber,
			&acc.HolderName,
			&acc.Balance,
			&acc.Currency,
			&acc.Status,
			&acc.CreatedAt,
			&acc.UpdatedAt,
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, errs.New(errs.CodeNotFound, fmt.Sprintf("account %s does not exist", accNum))
			}
			return nil, errs.Wrap(errs.CodeInternal, "failed to acquire row lock on account", err)
		}
		accountsMap[acc.AccountNumber] = &acc
	}

	sourceAcc := accountsMap[cmd.SourceAccount]
	destAcc := accountsMap[cmd.DestinationAcc]

	// 3. Domain Validations
	if sourceAcc.Currency != destAcc.Currency {
		return nil, errs.New(errs.CodeInvalidInput, fmt.Sprintf("currency mismatch: %s to %s requires FX conversion", sourceAcc.Currency, destAcc.Currency))
	}
	if cmd.Currency != "" && cmd.Currency != sourceAcc.Currency {
		return nil, errs.New(errs.CodeInvalidInput, fmt.Sprintf("command currency %s must match account currency %s", cmd.Currency, sourceAcc.Currency))
	}
	if sourceAcc.Status != domain.AccountStatusActive {
		return nil, errs.New(errs.CodeAccountFrozen, fmt.Sprintf("source account %s is %s", sourceAcc.AccountNumber, sourceAcc.Status))
	}
	if destAcc.Status != domain.AccountStatusActive {
		return nil, errs.New(errs.CodeAccountFrozen, fmt.Sprintf("destination account %s is %s", destAcc.AccountNumber, destAcc.Status))
	}
	// 4. Update balances using exact fixed-point arithmetic (zero IEEE-754 drift)
	newSourceBal, err := money.Sub(sourceAcc.Balance, cmd.Amount)
	if err != nil {
		if errors.Is(err, money.ErrInsufficient) {
			return nil, errs.New(errs.CodeInsufficient, fmt.Sprintf("insufficient balance in account %s (balance: %.4f, required: %.4f)", sourceAcc.AccountNumber, sourceAcc.Balance, cmd.Amount))
		}
		return nil, errs.New(errs.CodeInvalidInput, err.Error())
	}

	newDestBal, err := money.Add(destAcc.Balance, cmd.Amount)
	if err != nil {
		return nil, errs.New(errs.CodeInvalidInput, err.Error())
	}

	_, err = tx.Exec(ctx, `UPDATE accounts SET balance = $1, updated_at = NOW() WHERE id = $2`, newSourceBal, sourceAcc.ID)
	if err != nil {
		return nil, errs.Wrap(errs.CodeInternal, "failed to deduct source balance", err)
	}

	_, err = tx.Exec(ctx, `UPDATE accounts SET balance = $1, updated_at = NOW() WHERE id = $2`, newDestBal, destAcc.ID)
	if err != nil {
		return nil, errs.Wrap(errs.CodeInternal, "failed to credit destination balance", err)
	}

	// 5. Insert Transaction Header
	transactionID := uuid.New()
	createdAt := time.Now()
	_, err = tx.Exec(ctx, `
		INSERT INTO transactions (id, reference_id, description, status, created_at)
		VALUES ($1, $2, $3, $4, $5)
	`, transactionID, cmd.ReferenceID, cmd.Description, string(domain.TransactionStatusPosted), createdAt)
	if err != nil {
		return nil, errs.Wrap(errs.CodeInternal, "failed to record transaction header", err)
	}

	// 6. Insert Double-Entry Items (Standard Banking Liability Ledger Perspective):
	// Bank liability to Source account decreases -> DEBIT
	// Bank liability to Destination account increases -> CREDIT
	// Strict Invariant: SUM(DEBIT) = SUM(CREDIT) = cmd.Amount
	_, err = tx.Exec(ctx, `
		INSERT INTO entries (id, transaction_id, account_id, direction, amount, created_at)
		VALUES 
			($1, $2, $3, 'DEBIT', $4, $5),
			($6, $2, $7, 'CREDIT', $4, $5)
	`, uuid.New(), transactionID, sourceAcc.ID, cmd.Amount, createdAt,
		uuid.New(), destAcc.ID)
	if err != nil {
		return nil, errs.Wrap(errs.CodeInternal, "failed to insert ledger entries", err)
	}

	// 7. Insert Transactional Outbox Event (Atomic with the financial transfer!)
	// Crucial: aggregate_id is set to sourceAcc.AccountNumber so Kafka partitions by account key!
	eventPayload := map[string]interface{}{
		"transaction_id":      transactionID.String(),
		"reference_id":        cmd.ReferenceID,
		"source_account":      sourceAcc.AccountNumber,
		"destination_account": destAcc.AccountNumber,
		"amount":              cmd.Amount,
		"currency":            cmd.Currency,
		"description":         cmd.Description,
		"timestamp":           createdAt.Format(time.RFC3339),
		"trace_context":       telemetry.InjectMap(ctx),
	}
	payloadBytes, err := json.Marshal(eventPayload)
	if err != nil {
		return nil, errs.Wrap(errs.CodeInternal, "failed to serialize outbox event payload", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (id, event_type, aggregate_type, aggregate_id, payload, status, created_at, next_retry_at)
		VALUES ($1, 'transaction.created', 'TRANSACTION', $2, $3, 'PENDING', $4, $4)
	`, uuid.New(), sourceAcc.AccountNumber, payloadBytes, createdAt)
	if err != nil {
		return nil, errs.Wrap(errs.CodeInternal, "failed to record outbox event", err)
	}

	// 8. Commit ACID transaction
	if err := tx.Commit(ctx); err != nil {
		return nil, errs.Wrap(errs.CodeInternal, "failed to commit ledger transaction", err)
	}

	return &domain.Transaction{
		ID:          transactionID,
		ReferenceID: cmd.ReferenceID,
		Description: cmd.Description,
		Status:      domain.TransactionStatusPosted,
		CreatedAt:   createdAt,
	}, nil
}
