package repository

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// databaseError is a failed database call. Its text is safe to log; the cause
// stays reachable through Unwrap for errors.Is and errors.As.
type databaseError struct {
	what  string
	cause error
}

func (e *databaseError) Error() string { return e.what + ": " + describe(e.cause) }

func (e *databaseError) Unwrap() error { return e.cause }

// dbError wraps the error of a database call with what was being done.
func dbError(what string, cause error) error {
	return &databaseError{what: what, cause: cause}
}

// describe prints a database error without any value of the row. A server error
// is printed from its SQLSTATE and, where the message cannot quote a value, from
// its message; the detail, hint and context never are. The message of a data
// exception (class 22), an integrity violation (23) or a syntax or access error
// (42) can quote the value, so those print the code alone. Any other error, such
// as a refused connection, is printed as it is.
func describe(err error) string {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err.Error()
	}

	switch pgErr.Code[:min(2, len(pgErr.Code))] {
	case "22", "23", "42":
		return fmt.Sprintf("SQLSTATE %s", pgErr.Code)
	}

	return fmt.Sprintf("%s (SQLSTATE %s)", pgErr.Message, pgErr.Code)
}
