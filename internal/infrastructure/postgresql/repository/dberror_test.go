package repository

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A database error reaches a log. The server can quote the failing value in the
// message of some classes and always puts it in the detail, so what is printed
// is decided by class, and the cause stays reachable for errors.Is and errors.As.
func TestDatabaseErrorPrintsNoValue(t *testing.T) {
	const secret = "card 4111 1111"

	pgError := func(code, message string) *pgconn.PgError {
		return &pgconn.PgError{
			Code: code, Message: message, Detail: "Failing row contains (" + secret + ")",
			Hint: "hint " + secret, Where: "where " + secret, ColumnName: "text", ConstraintName: "c",
		}
	}

	tests := []struct {
		name     string
		cause    error
		wantText string
	}{
		{name: "connectionException", cause: pgError("08006", "connection failure"), wantText: "connection failure (SQLSTATE 08006)"},
		{name: "serializationFailure", cause: pgError("40001", "could not serialize access"), wantText: "could not serialize access (SQLSTATE 40001)"},
		{name: "tooManyConnections", cause: pgError("53300", "too many connections"), wantText: "too many connections (SQLSTATE 53300)"},
		{name: "adminShutdown", cause: pgError("57P01", "terminating connection"), wantText: "terminating connection (SQLSTATE 57P01)"},
		// These classes can quote the value in the message itself.
		{name: "dataException", cause: pgError("22P02", `invalid input syntax for type integer: "`+secret+`"`), wantText: "SQLSTATE 22P02"},
		{name: "integrityViolation", cause: pgError("23505", `duplicate key value "`+secret+`"`), wantText: "SQLSTATE 23505"},
		{name: "syntaxOrAccess", cause: pgError("42703", `column "`+secret+`" does not exist`), wantText: "SQLSTATE 42703"},
		{name: "notADatabaseError", cause: errors.New("dial tcp 127.0.0.1:5432: connection refused"), wantText: "dial tcp 127.0.0.1:5432: connection refused"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := dbError("read inbox batch", fmt.Errorf("wrapped by the driver: %w", tt.cause))

			assert.Contains(t, err.Error(), "read inbox batch: ")
			assert.Contains(t, err.Error(), tt.wantText)
			assert.NotContains(t, err.Error(), secret)
			assert.ErrorIs(t, err, tt.cause, "the cause stays reachable")

			var pgErr *pgconn.PgError
			if errors.As(tt.cause, &pgErr) {
				require.ErrorAs(t, err, &pgErr)
			}
		})
	}
}
