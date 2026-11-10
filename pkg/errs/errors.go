package errs

import (
	"errors"
	"fmt"
)

type ErrorCode string

const (
	CodeNotFound       ErrorCode = "NOT_FOUND"
	CodeAlreadyExists  ErrorCode = "ALREADY_EXISTS"
	CodeConflict       ErrorCode = "CONFLICT"
	CodeInvalidInput   ErrorCode = "INVALID_INPUT"
	CodeInsufficient   ErrorCode = "INSUFFICIENT_FUNDS"
	CodeAccountFrozen  ErrorCode = "ACCOUNT_FROZEN"
	CodeInternal       ErrorCode = "INTERNAL_ERROR"
)

// AppError represents a structured domain error
type AppError struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
	Err     error     `json:"-"`
}

func (e *AppError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

func (e *AppError) Unwrap() error {
	return e.Err
}

func New(code ErrorCode, message string) *AppError {
	return &AppError{Code: code, Message: message}
}

func Wrap(code ErrorCode, message string, err error) *AppError {
	return &AppError{Code: code, Message: message, Err: err}
}

// Helpers for checking error codes
func Is(err error, code ErrorCode) bool {
	var appErr *AppError
	if errors.As(err, &appErr) {
		return appErr.Code == code
	}
	return false
}
