package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// UserInsert holds the columns set when creating a user. PasswordHash is already in
// the web2py format; empty, the account cannot sign in until a password is set.
type UserInsert struct {
	Email        string
	Username     *string
	FirstName    *string
	LastName     *string
	PhoneWork    *string
	PasswordHash *string
}

func (oDb *DB) userIDBy(ctx context.Context, column, value string) (int64, bool, error) {
	var id int64
	err := oDb.DB.QueryRowContext(ctx, "SELECT id FROM auth_user WHERE "+column+" = ? LIMIT 1", value).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("user by %s: %w", column, err)
	}
	return id, true, nil
}

// UserIDByEmail returns the id of the user with this email, if any.
func (oDb *DB) UserIDByEmail(ctx context.Context, email string) (int64, bool, error) {
	return oDb.userIDBy(ctx, "email", email)
}

// UserIDByUsername returns the id of the user with this username, if any.
func (oDb *DB) UserIDByUsername(ctx context.Context, username string) (int64, bool, error) {
	return oDb.userIDBy(ctx, "username", username)
}

// InsertUserWithPrivateGroup creates a user, then its private group "user_<id>" and
// the membership, as web2py does on registration (auth.settings.create_user_groups).
// Meant to run in a transaction.
func (oDb *DB) InsertUserWithPrivateGroup(ctx context.Context, u UserInsert) (int64, error) {
	res, err := oDb.ExecContext(ctx,
		`INSERT INTO auth_user (email, username, first_name, last_name, phone_work, password)
		VALUES (?, ?, ?, ?, ?, ?)`,
		u.Email, u.Username, u.FirstName, u.LastName, u.PhoneWork, u.PasswordHash)
	if err != nil {
		return 0, fmt.Errorf("InsertUserWithPrivateGroup user: %w", err)
	}
	userID, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("InsertUserWithPrivateGroup user id: %w", err)
	}
	res, err = oDb.ExecContext(ctx,
		"INSERT INTO auth_group (role, description, privilege) VALUES (?, ?, 'F')",
		fmt.Sprintf("user_%d", userID), fmt.Sprintf("group uniquely assigned to user %d", userID))
	if err != nil {
		return 0, fmt.Errorf("InsertUserWithPrivateGroup group: %w", err)
	}
	groupID, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("InsertUserWithPrivateGroup group id: %w", err)
	}
	if _, err := oDb.ExecContext(ctx,
		"INSERT INTO auth_membership (user_id, group_id, primary_group) VALUES (?, ?, 'F')",
		userID, groupID); err != nil {
		return 0, fmt.Errorf("InsertUserWithPrivateGroup membership: %w", err)
	}
	oDb.SetChange("auth_user")
	oDb.SetChange("auth_group")
	oDb.SetChange("auth_membership")
	return userID, nil
}
