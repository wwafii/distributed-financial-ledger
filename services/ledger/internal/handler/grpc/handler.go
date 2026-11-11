package grpc

import (
	"context"
	"time"

	ledgerv1 "github.com/wwafii/distributed-financial-ledger/api/proto/gen/ledger/v1"
	"github.com/wwafii/distributed-financial-ledger/pkg/errs"
	"github.com/wwafii/distributed-financial-ledger/pkg/telemetry"
	"github.com/wwafii/distributed-financial-ledger/services/ledger/internal/domain"
	"github.com/wwafii/distributed-financial-ledger/services/ledger/internal/repository/postgres"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type LedgerHandler struct {
	ledgerv1.UnimplementedLedgerServiceServer
	repo *postgres.LedgerRepository
}

func NewLedgerHandler(repo *postgres.LedgerRepository) *LedgerHandler {
	return &LedgerHandler{repo: repo}
}

func (h *LedgerHandler) CreateTransaction(ctx context.Context, req *ledgerv1.CreateTransactionRequest) (*ledgerv1.CreateTransactionResponse, error) {
	ctx = telemetry.ExtractGRPC(ctx)
	tracer := telemetry.Tracer("ledger-service")
	ctx, span := tracer.Start(ctx, "gRPC CreateTransaction")
	defer span.End()

	if req.ReferenceId == "" {
		return nil, status.Error(codes.InvalidArgument, "reference_id is required")
	}
	if req.SourceAccountNumber == "" || req.DestinationAccountNumber == "" {
		return nil, status.Error(codes.InvalidArgument, "source and destination account numbers are required")
	}
	if req.Amount <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount must be positive")
	}

	cmd := &domain.TransferCommand{
		ReferenceID:    req.ReferenceId,
		SourceAccount:  req.SourceAccountNumber,
		DestinationAcc: req.DestinationAccountNumber,
		Amount:         req.Amount,
		Currency:       req.Currency,
		Description:    req.Description,
	}
	if cmd.Currency == "" {
		cmd.Currency = "IDR"
	}

	tx, err := h.repo.ExecuteTransfer(ctx, cmd)
	if err != nil {
		if errs.Is(err, errs.CodeNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		if errs.Is(err, errs.CodeInsufficient) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		if errs.Is(err, errs.CodeAccountFrozen) {
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		if errs.Is(err, errs.CodeInvalidInput) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	telemetry.TransactionsTotal.WithLabelValues("POSTED").Inc()

	return &ledgerv1.CreateTransactionResponse{
		TransactionId: tx.ID.String(),
		ReferenceId:   tx.ReferenceID,
		Status:        string(tx.Status),
		CreatedAt:     tx.CreatedAt.Format(time.RFC3339),
	}, nil
}

func (h *LedgerHandler) GetAccountBalance(ctx context.Context, req *ledgerv1.GetAccountBalanceRequest) (*ledgerv1.GetAccountBalanceResponse, error) {
	ctx = telemetry.ExtractGRPC(ctx)
	tracer := telemetry.Tracer("ledger-service")
	ctx, span := tracer.Start(ctx, "gRPC GetAccountBalance")
	defer span.End()

	if req.AccountNumber == "" {
		return nil, status.Error(codes.InvalidArgument, "account_number is required")
	}

	acc, err := h.repo.GetAccountByNumber(ctx, req.AccountNumber)
	if err != nil {
		if errs.Is(err, errs.CodeNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &ledgerv1.GetAccountBalanceResponse{
		AccountId:     acc.ID.String(),
		AccountNumber: acc.AccountNumber,
		HolderName:    acc.HolderName,
		Balance:       acc.Balance,
		Currency:      acc.Currency,
		Status:        string(acc.Status),
	}, nil
}

func (h *LedgerHandler) FreezeAccount(ctx context.Context, req *ledgerv1.FreezeAccountRequest) (*ledgerv1.FreezeAccountResponse, error) {
	ctx = telemetry.ExtractGRPC(ctx)
	tracer := telemetry.Tracer("ledger-service")
	ctx, span := tracer.Start(ctx, "gRPC FreezeAccount")
	defer span.End()

	if req.AccountNumber == "" {
		return nil, status.Error(codes.InvalidArgument, "account_number is required")
	}

	err := h.repo.FreezeAccount(ctx, req.AccountNumber, req.Reason)
	if err != nil {
		if errs.Is(err, errs.CodeNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &ledgerv1.FreezeAccountResponse{
		Success:       true,
		AccountNumber: req.AccountNumber,
		Status:        string(domain.AccountStatusFrozen),
		Message:       "account successfully frozen",
	}, nil
}
