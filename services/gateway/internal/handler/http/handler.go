package http

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"time"

	ledgerv1 "github.com/wwafii/distributed-financial-ledger/api/proto/gen/ledger/v1"
	"github.com/wwafii/distributed-financial-ledger/pkg/auth"
	"github.com/wwafii/distributed-financial-ledger/pkg/telemetry"
	"github.com/wwafii/distributed-financial-ledger/services/gateway/internal/idempotency"
	otelcodes "go.opentelemetry.io/otel/codes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GatewayHandler struct {
	ledgerClient ledgerv1.LedgerServiceClient
	idempEngine  *idempotency.Engine
	logger       *slog.Logger
	jwtSecret    []byte
}

func NewGatewayHandler(
	ledgerClient ledgerv1.LedgerServiceClient,
	idempEngine *idempotency.Engine,
	logger *slog.Logger,
	jwtSecret ...[]byte,
) *GatewayHandler {
	secret := []byte(os.Getenv("JWT_SECRET"))
	if len(secret) == 0 {
		secret = []byte("default-financial-ledger-jwt-secret-key-32b")
	}
	if len(jwtSecret) > 0 && len(jwtSecret[0]) > 0 {
		secret = jwtSecret[0]
	}
	return &GatewayHandler{
		ledgerClient: ledgerClient,
		idempEngine:  idempEngine,
		logger:       logger,
		jwtSecret:    secret,
	}
}

func (h *GatewayHandler) JWTSecret() []byte {
	return h.jwtSecret
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
	ctx := telemetry.ExtractHTTP(r)
	tracer := telemetry.Tracer("api-gateway")
	ctx, span := tracer.Start(ctx, "HTTP POST /api/v1/transactions")
	defer span.End()

	idempKey := r.Header.Get("Idempotency-Key")
	if idempKey == "" {
		span.SetStatus(otelcodes.Error, "missing idempotency key")
		writeJSON(w, http.StatusBadRequest, StandardResponse{
			Success: false,
			Error:   "Missing required 'Idempotency-Key' header",
		})
		return
	}

	// 1. Bound request body size (Max 1MB) to protect against DoS OOM
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "failed to read body")
		writeJSON(w, http.StatusBadRequest, StandardResponse{
			Success: false,
			Error:   "Failed to read request body or payload exceeds maximum allowed size (1MB)",
		})
		return
	}

	// 2. Parse and execute basic input validation at ingress boundary
	var reqBody CreateTransactionHTTPRequest
	if err := json.Unmarshal(bodyBytes, &reqBody); err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "invalid JSON")
		writeJSON(w, http.StatusBadRequest, StandardResponse{
			Success: false,
			Error:   "Invalid JSON payload: " + err.Error(),
		})
		return
	}

	if reqBody.SourceAccount == "" || reqBody.DestinationAcc == "" {
		writeJSON(w, http.StatusBadRequest, StandardResponse{
			Success: false,
			Error:   "source_account and destination_account are required",
		})
		return
	}
	if reqBody.SourceAccount == reqBody.DestinationAcc {
		writeJSON(w, http.StatusBadRequest, StandardResponse{
			Success: false,
			Error:   "cannot transfer to the same account",
		})
		return
	}
	if math.IsNaN(reqBody.Amount) || math.IsInf(reqBody.Amount, 0) || reqBody.Amount <= 0 {
		writeJSON(w, http.StatusBadRequest, StandardResponse{
			Success: false,
			Error:   "transfer amount must be a valid strictly positive number",
		})
		return
	}

	if reqBody.Currency == "" {
		reqBody.Currency = "IDR"
	}

	// 2.1 BOLA / IDOR Defense: Verify Account Ownership if authenticated
	claims := auth.FromContext(r.Context())
	if claims != nil {
		if claims.Role != auth.RoleAdmin && claims.Role != auth.RoleService && claims.AccountNumber != reqBody.SourceAccount {
			span.SetStatus(otelcodes.Error, "unauthorized account access (IDOR blocked)")
			writeJSON(w, http.StatusForbidden, StandardResponse{
				Success: false,
				Error:   fmt.Sprintf("Forbidden: unauthorized to initiate transactions from source_account '%s'", reqBody.SourceAccount),
			})
			return
		}
	}

	// 3. Compute SHA-256 payload fingerprint (Stripe Idempotency Standard)
	hash := sha256.Sum256(bodyBytes)
	payloadHash := hex.EncodeToString(hash[:])

	// 4. Check or Acquire Idempotency Lock via Redis with payload fingerprinting
	lockResult, err := h.idempEngine.AcquireWithPayload(ctx, idempKey, payloadHash, 60*time.Second)
	if err != nil {
		if errors.Is(err, idempotency.ErrPayloadMismatch) {
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, "payload mismatch on idempotency key")
			writeJSON(w, http.StatusBadRequest, StandardResponse{
				Success: false,
				Error:   "Idempotency-Key reused with different request payload",
			})
			return
		}
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "idempotency evaluation failure")
		h.logger.Error("idempotency check failure", "error", err)
		writeJSON(w, http.StatusInternalServerError, StandardResponse{
			Success: false,
			Error:   "Internal server error while evaluating idempotency lock",
		})
		return
	}

	// 5. Handle Replay if key was already seen
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

		// Defensive check: Corrupted or unrecognized lock record
		span.SetStatus(otelcodes.Error, "corrupted idempotency state")
		writeJSON(w, http.StatusInternalServerError, StandardResponse{
			Success: false,
			Error:   "Invalid or corrupted idempotency record state",
		})
		return
	}

	// 6. First time execution: Call Core Ledger Service via gRPC
	grpcReq := &ledgerv1.CreateTransactionRequest{
		ReferenceId:              idempKey,
		SourceAccountNumber:      reqBody.SourceAccount,
		DestinationAccountNumber: reqBody.DestinationAcc,
		Amount:                   reqBody.Amount,
		Currency:                 reqBody.Currency,
		Description:              reqBody.Description,
	}

	grpcResp, err := h.ledgerClient.CreateTransaction(telemetry.InjectGRPC(ctx), grpcReq)
	if err != nil {
		// Use uncancelled context with dedicated timeout so lock cleanup succeeds even if client disconnected
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		_ = h.idempEngine.Release(cleanupCtx, idempKey)
		cancel()

		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
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

	span.SetStatus(otelcodes.Ok, "success")

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
	ctx := telemetry.ExtractHTTP(r)
	tracer := telemetry.Tracer("api-gateway")
	ctx, span := tracer.Start(ctx, "HTTP GET /api/v1/accounts/balance")
	defer span.End()

	accNum := r.PathValue("account_number")
	if accNum == "" {
		writeJSON(w, http.StatusBadRequest, StandardResponse{Success: false, Error: "Missing account_number"})
		return
	}

	// BOLA / IDOR Defense: Verify Account Ownership if authenticated
	claims := auth.FromContext(r.Context())
	if claims != nil {
		if claims.Role != auth.RoleAdmin && claims.Role != auth.RoleService && claims.AccountNumber != accNum {
			span.SetStatus(otelcodes.Error, "unauthorized balance access (IDOR blocked)")
			writeJSON(w, http.StatusForbidden, StandardResponse{
				Success: false,
				Error:   fmt.Sprintf("Forbidden: unauthorized to view balance for account '%s'", accNum),
			})
			return
		}
	}

	grpcResp, err := h.ledgerClient.GetAccountBalance(telemetry.InjectGRPC(ctx), &ledgerv1.GetAccountBalanceRequest{
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
	ctx := telemetry.ExtractHTTP(r)
	tracer := telemetry.Tracer("api-gateway")
	ctx, span := tracer.Start(ctx, "HTTP POST /api/v1/accounts/freeze")
	defer span.End()

	accNum := r.PathValue("account_number")
	if accNum == "" {
		writeJSON(w, http.StatusBadRequest, StandardResponse{Success: false, Error: "Missing account_number"})
		return
	}

	// BFLA Defense: Enforce Administrator Privileges
	claims := auth.FromContext(r.Context())
	if claims != nil {
		if claims.Role != auth.RoleAdmin && claims.Role != auth.RoleService {
			span.SetStatus(otelcodes.Error, "unauthorized admin operation (BFLA blocked)")
			writeJSON(w, http.StatusForbidden, StandardResponse{
				Success: false,
				Error:   "Forbidden: administrator privileges required to freeze accounts",
			})
			return
		}
	}

	grpcResp, err := h.ledgerClient.FreezeAccount(telemetry.InjectGRPC(ctx), &ledgerv1.FreezeAccountRequest{
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

type IssueTokenHTTPRequest struct {
	AccountNumber string `json:"account_number"`
	Role          string `json:"role,omitempty"` // "customer", "admin"
	Secret        string `json:"secret,omitempty"`
}

// HandleIssueToken issues a signed JWT token for authentication
func (h *GatewayHandler) HandleIssueToken(w http.ResponseWriter, r *http.Request) {
	var req IssueTokenHTTPRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, StandardResponse{
			Success: false,
			Error:   "Invalid JSON payload: " + err.Error(),
		})
		return
	}

	if req.AccountNumber == "" {
		writeJSON(w, http.StatusBadRequest, StandardResponse{
			Success: false,
			Error:   "account_number is required",
		})
		return
	}

	if req.Role == "" {
		req.Role = auth.RoleCustomer
	}

	// If requesting admin role, enforce admin secret
	if req.Role == auth.RoleAdmin {
		expectedAdminSecret := os.Getenv("ADMIN_SECRET")
		if expectedAdminSecret == "" {
			expectedAdminSecret = "admin-secret-dev-key"
		}
		if req.Secret != expectedAdminSecret {
			writeJSON(w, http.StatusUnauthorized, StandardResponse{
				Success: false,
				Error:   "Unauthorized: invalid admin secret",
			})
			return
		}
	}

	exp := time.Now().Add(24 * time.Hour).Unix()
	claims := auth.Claims{
		Subject:       req.AccountNumber,
		AccountNumber: req.AccountNumber,
		Role:          req.Role,
		IssuedAt:      time.Now().Unix(),
		ExpiresAt:     exp,
	}

	token, err := auth.SignToken(claims, h.jwtSecret)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, StandardResponse{
			Success: false,
			Error:   "Failed to sign auth token: " + err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, StandardResponse{
		Success: true,
		Data: map[string]interface{}{
			"token":          token,
			"account_number": req.AccountNumber,
			"role":           req.Role,
			"expires_at":     exp,
		},
	})
}

// HandleHealthz returns HTTP 200 OK for K8s liveness/readiness probes
func (h *GatewayHandler) HandleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "UP",
		"time":   time.Now().Format(time.RFC3339),
	})
}
