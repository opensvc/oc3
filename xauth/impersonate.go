package xauth

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/shaj13/go-guardian/v2/auth"
)

// ErrUnknownUser tells that the user to impersonate does not exist.
var ErrUnknownUser = errors.New("unknown user")

const queryUserByIdent = `SELECT auth_user.id, auth_user.email
		FROM auth_user
		WHERE auth_user.id = ? OR auth_user.email = ?
		LIMIT 1`

// Querier is the part of *sql.DB, or of a transaction, the identity lookups use.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// LoadUserInfo returns the identity of the user designated by an auth_user.id or
// an email, built as the web2py basic strategy builds it after a sign-in: same
// name, id, groups and extensions, without the password hash, which an
// impersonation never checks.
func LoadUserInfo(ctx context.Context, db Querier, ident string) (auth.Info, error) {
	id, isID := ParseUserID(ident)
	if !isID && !strings.Contains(ident, "@") {
		return nil, fmt.Errorf("%w: %s", ErrUnknownUser, ident)
	}
	var u authWeb2py
	err := db.QueryRowContext(ctx, queryUserByIdent, id, ident).Scan(&u.id, &u.email)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrUnknownUser, ident)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	ext := make(auth.Extensions)
	ext.Set(XUserID, u.id)
	ext.Set(XUserEmail, u.email)
	return auth.NewUserInfo(u.email, u.id, u.Groups(ctx, db, u.email), ext), nil
}

// ParseUserID parses an auth_user.id given as text.
func ParseUserID(s string) (int64, bool) {
	id, err := strconv.ParseInt(s, 10, 64)
	return id, err == nil && id > 0
}
