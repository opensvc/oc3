package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// UserFilterset returns the session filterset of a user, the one the historical
// collector applies to every list (gen_filterset_user): its id and name, or 0 when
// the user has none.
func (oDb *DB) UserFilterset(ctx context.Context, userID int64) (int, string, error) {
	var (
		id   int
		name string
	)
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT gen_filtersets.id, gen_filtersets.fset_name FROM gen_filterset_user"+
			" JOIN gen_filtersets ON gen_filtersets.id = gen_filterset_user.fset_id"+
			" WHERE gen_filterset_user.user_id = ? ORDER BY gen_filterset_user.id DESC LIMIT 1", userID).
		Scan(&id, &name)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, "", nil
	case err != nil:
		return 0, "", fmt.Errorf("userFilterset: %w", err)
	}
	return id, name, nil
}

// SetUserFilterset makes a filterset the session filterset of a user, in place of
// the previous one, as the historical collector does (one row per user).
func (oDb *DB) SetUserFilterset(ctx context.Context, userID int64, fsetID int) error {
	if _, err := oDb.ExecContext(ctx, "DELETE FROM gen_filterset_user WHERE user_id = ?", userID); err != nil {
		return fmt.Errorf("setUserFilterset: %w", err)
	}
	if _, err := oDb.ExecContext(ctx,
		"INSERT INTO gen_filterset_user (user_id, fset_id) VALUES (?, ?)", userID, fsetID); err != nil {
		return fmt.Errorf("setUserFilterset: %w", err)
	}
	oDb.SetChange("gen_filterset_user")
	return nil
}

// ClearUserFilterset removes the session filterset of a user: the lists show
// everything the user may see again.
func (oDb *DB) ClearUserFilterset(ctx context.Context, userID int64) error {
	if _, err := oDb.ExecContext(ctx, "DELETE FROM gen_filterset_user WHERE user_id = ?", userID); err != nil {
		return fmt.Errorf("clearUserFilterset: %w", err)
	}
	oDb.SetChange("gen_filterset_user")
	return nil
}
