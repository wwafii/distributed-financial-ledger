package http

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	ledgerv1 "github.com/wwafii/distributed-financial-ledger/api/proto/gen/ledger/v1"
	"github.com/wwafii/distributed-financial-ledger/services/gateway/internal/idempotency"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GatewayHandler struct {
	ledgerClient ledgerv1.LedgerServiceClient
	idempEngine  *idempotency.Engine
	logger       *slog.Logger
}

func NewGatewayHandler(
	ledgerClient ledgerv1.LedgerServiceClient,
	idempEngine *idempotency.Engine,
	logger *slog.Logger,
) *GatewayHandler {
	return &GatewayHandler{
		ledgerClient: ledgerClient,
		idempEngine:  idempEngine,
		logger:       logger,
	}
}

type CreateTransactionHTTPRequest struct {
	SourceAccount  string  `json:"source_account"`
	DestinationAcc string  `json:"destination_account"`
	Amount         float64 `json:"amount"`
	Currency       string  `json:"currency"`
	Description    string  `json:"description"`
}

type StandardResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

func writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}

// HandleCreateTransaction handles financial transfers protected by Idempotency-Key
func (h *GatewayHandler) HandleCreateTransaction(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	idempKey := r.Header.Get("Idempotency-Key")

	if idempKey == "" {
		writeJSON(w, http.StatusBadRequest, StandardResponse{
			Success: false,
			Error:   "Missing required 'Idempotency-Key' header",
		})
		return
	}

	// 1. Check or Acquire Idempotency Lock via Redis
	lockResult, err := h.idempEngine.AcquireOrGet(ctx, idempKey, 60*time.Second)
	if err != nil {
		h.logger.Error("idempotency check failure", "error", err)
		writeJSON(w, http.StatusInternalServerError, StandardResponse{
			Success: false,
			Error:   "Internal server error while evaluating idempotency lock",
		})
		return
	}

	// 2. Handle Replay if key was already seen
	if !lockResult.Acquired {
		record := lockResult.Record
		if record.Status == idempotency.StatusProcessing {
			w.Header().Set("Retry-After", "2")
			writeJSON(w, http.StatusConflict, StandardResponse{
				Success: false,
				Error:   "Transaction is currently in-flight. Please wait before retrying.",
			})
			return
		}

		if record.Status == idempotency.StatusCompleted {
			// Replay cached response directly from Redis!
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Idempotent-Replay", "true")
			w.WriteHeader(record.StatusCode)
			_, _ = w.Write(record.Body)
			return
		}
	}

	// 3. First time execution: Process Transfer Request
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		_ = h.idempEngine.Release(ctx, idempKey)
		writeJSON(w, http.StatusBadRequest, StandardResponse{
			Success: false,
			Error:   "Failed to read request body",
		})
		return
	}

	var reqBody CreateTransactionHTTPRequest
	if err := json.Unmarshal(bodyBytes, &reqBody); err != nil {
		_ = h.idempEngine.Release(ctx, idempKey)
		writeJSON(w, http.StatusBadRequest, StandardResponse{
			Success: false,
			Error:   "Invalid JSON payload: " + err.Error(),
		})
		return
	}

	if reqBody.Currency == "" {
		reqBody.Currency = "IDR"
	}

	// 4. Call Core Ledger Service via gRPC
	grpcReq := &ledgerv1.CreateTransactionRequest{
		ReferenceId:                idempKey,
		SourceAccountNumber:        reqBody.SourceAccount,
		DestinationAccountNumber:   reqBody.DestinationAcc,
		Amount:                     reqBody.Amount,
		Currency:                   reqBody.Currency,
		Description:                reqBody.Description,
	}

	grpcResp, err := h.ledgerClient.CreateTransaction(ctx, grpcReq)
	if err != nil {
		_ = h.idempEngine.Release(ctx, idempKey)
		grpcStatus, _ := status.FromError(err)

		var httpCode int
		switch grpcStatus.Code() {
		case codes.NotFound:
			httpCode = http.StatusNotFound
		case codes.InvalidArgument:
			httpCode = http.StatusBadRequest
		case codes.FailedPrecondition: // Insufficient funds
			httpCode = http.StatusUnprocessableEntity
		case codes.PermissionDenied: // Account frozen
			httpCode = http.StatusForbidden
		default:
			httpCode = http.StatusInternalServerError
		}

		writeJSON(w, httpCode, StandardResponse{
			Success: false,
			Error:   grpcStatus.Message(),
		})
		return
	}

	// 5. Success: Cache final response in Redis for future replays
	respPayload := StandardResponse{
		Success: true,
		Data: map[string]interface{}{
			"transaction_id": grpcResp.TransactionId,
			"reference_id":   grpcResp.ReferenceId,
			"status":         grpcResp.Status,
			"created_at":     grpcResp.CreatedAt,
		},
	}
	respBytes, _ := json.Marshal(respPayload)

	// Cache completed transaction for 24 hours
	_ = h.idempEngine.SaveResponse(ctx, idempKey, http.StatusCreated, respBytes, 24*time.Hour)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(respBytes)
}

// HandleGetBalance retrieves current balance from ledger
func (h *GatewayHandler) HandleGetBalance(w http.ResponseWriter, r *http.Request) {
	accNum := r.PathValue("account_number")
	if accNum == "" {
		writeJSON(w, http.StatusBadRequest, StandardResponse{Success: false, Error: "Missing account_number"})
		return
	}

	grpcResp, err := h.ledgerClient.GetAccountBalance(r.Context(), &ledgerv1.GetAccountBalanceRequest{
		AccountNumber: accNum,
	})
	if err != nil {
		grpcStatus, _ := status.FromError(err)
		httpCode := http.StatusInternalServerError
		if grpcStatus.Code() == codes.NotFound {
			httpCode = http.StatusNotFound
		}
		writeJSON(w, httpCode, StandardResponse{Success: false, Error: grpcStatus.Message()})
		return
	}

	writeJSON(w, http.StatusOK, StandardResponse{
		Success: true,
		Data: map[string]interface{}{
			"account_id":     grpcResp.AccountId,
			"account_number": grpcResp.AccountNumber,
			"holder_name":    grpcResp.HolderName,
			"balance":        grpcResp.Balance,
			"currency":       grpcResp.Currency,
			"status":         grpcResp.Status,
		},
	})
}

// HandleFreezeAccount freezes an account manually or via rule
func (h *GatewayHandler) HandleFreezeAccount(w http.ResponseWriter, r *http.Request) {
	accNum := r.PathValue("account_number")
	if accNum == "" {
		writeJSON(w, http.StatusBadRequest, StandardResponse{Success: false, Error: "Missing account_number"})
		return
	}

	grpcResp, err := h.ledgerClient.FreezeAccount(r.Context(), &ledgerv1.FreezeAccountRequest{
		AccountNumber: accNum,
		Reason:        "Manual freeze from gateway",
	})
	if err != nil {
		grpcStatus, _ := status.FromError(err)
		writeJSON(w, http.StatusInternalServerError, StandardResponse{Success: false, Error: grpcStatus.Message()})
		return
	}

	writeJSON(w, http.StatusOK, StandardResponse{
		Success: true,
		Data:    grpcResp,
	})
}

// HandleHealthz returns HTTP 200 OK for K8s liveness/readiness probes
func (h *GatewayHandler) HandleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "UP",
		"time":   time.Now().Format(time.RFC3339),
	})
}
