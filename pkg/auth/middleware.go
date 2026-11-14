package auth

import (
	"encoding/json"
	"net/http"
	"strings"
)

type errorResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
}

func writeAuthError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(errorResponse{
		Success: false,
		Error:   msg,
	})
}

// RequireAuth returns an HTTP middleware enforcing valid JWT Bearer token
func RequireAuth(secret []byte) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				writeAuthError(w, http.StatusUnauthorized, "Missing required Authorization header")
				return
			}

			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
				writeAuthError(w, http.StatusUnauthorized, "Invalid Authorization header format. Expected 'Bearer <token>'")
				return
			}

			claims, err := ParseToken(parts[1], secret)
			if err != nil {
				writeAuthError(w, http.StatusUnauthorized, "Unauthorized: "+err.Error())
				return
			}

			ctx := WithClaims(r.Context(), claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireRole returns an HTTP middleware verifying caller has at least one of the allowed roles
func RequireRole(allowedRoles ...string) func(http.Handler) http.Handler {
	allowedMap := make(map[string]struct{}, len(allowedRoles))
	for _, role := range allowedRoles {
		allowedMap[role] = struct{}{}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims := FromContext(r.Context())
			if claims == nil {
				writeAuthError(w, http.StatusUnauthorized, "Unauthorized: authentication required")
				return
			}

			if _, ok := allowedMap[claims.Role]; !ok {
				writeAuthError(w, http.StatusForbidden, "Forbidden: insufficient permissions for this operation")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
