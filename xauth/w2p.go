package xauth

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"

	"github.com/shaj13/go-guardian/v2/auth"
	"github.com/shaj13/go-guardian/v2/auth/strategies/basic"
)

type (
	authWeb2py struct {
		id       string
		email    string
		password string
	}
)

var (
	digestAlgBySize = map[int]string{
		128 / 4: "md5",
		160 / 4: "sha1",
		224 / 4: "sha224",
		256 / 4: "sha256",
		384 / 4: "sha384",
		512 / 4: "sha512",
	}
)

const (
	XUserID       string = "id"
	XUserEmail    string = "email"
	XUserPassword string = "password"
)

const (
	queryAuthWeb2py = `SELECT auth_user.id, auth_user.email, auth_user.password
		FROM auth_user 
		WHERE auth_user.email = ?`

	queryUserGroups = `SELECT auth_group.role
		FROM auth_membership
		JOIN auth_group ON auth_group.id = auth_membership.group_id
		JOIN auth_user ON auth_user.id = auth_membership.user_id
		WHERE auth_user.email = ?`
)

func NewBasicWeb2py(db *sql.DB, hmacKey string) auth.Strategy {
	authFunc := func(ctx context.Context, r *http.Request, userName, password string) (auth.Info, error) {
		u, err := authenticateWeb2py(ctx, db, userName, password, hmacKey)
		if err != nil {
			return nil, fmt.Errorf("invalid credentials")
		}
		return auth.NewUserInfo(userName, u.id, u.Groups(ctx, db, userName), u.extensions()), nil
	}
	return basic.New(authFunc)
}

func authenticateWeb2py(ctx context.Context, db *sql.DB, email, password, hmacKey string) (*authWeb2py, error) {
	var user authWeb2py

	err := db.
		QueryRowContext(ctx, queryAuthWeb2py, email).
		Scan(&user.id, &user.email, &user.password)
	if err != nil {
		return nil, fmt.Errorf("invalid credentials")
	}

	if !verifyWeb2pyPassword(password, user.password, hmacKey) {
		return nil, fmt.Errorf("invalid credentials")
	}

	return &user, nil
}

func (n *authWeb2py) extensions() auth.Extensions {
	ext := make(auth.Extensions)
	ext.Set(XUserID, n.id)
	ext.Set(XUserEmail, n.email)
	ext.Set(XUserPassword, n.password)
	return ext
}

func (n *authWeb2py) Groups(ctx context.Context, db *sql.DB, username string) []string {
	rows, err := db.QueryContext(ctx, queryUserGroups, username)
	if err != nil {
		return []string{}
	}
	defer rows.Close()

	groups := []string{}
	for rows.Next() {
		var group string
		if err := rows.Scan(&group); err != nil {
			continue
		}
		groups = append(groups, group)
	}
	return groups
}

func guessAlg(s string) string {
	n := len(s)
	alg, ok := digestAlgBySize[n]
	if ok {
		return alg
	}
	return ""
}

func toMD5(b []byte) []byte {
	a := md5.Sum(b)
	return a[:]
}

func toHMACSHA512(secret, b []byte) []byte {
	h := hmac.New(sha512.New, secret)
	h.Write(b)
	return h.Sum(nil)
}

func toSHA512(b []byte) []byte {
	a := sha512.Sum512(b)
	return a[:]
}

func toSHA384(b []byte) []byte {
	a := sha512.Sum384(b)
	return a[:]
}

func toSHA256(b []byte) []byte {
	a := sha256.Sum256(b)
	return a[:]
}

func toSHA224(b []byte) []byte {
	a := sha256.Sum224(b)
	return a[:]
}

func toSHA1(b []byte) []byte {
	a := sha1.Sum(b)
	return a[:]
}

func verifyWeb2pyPassword(password, storedHash, hmacKey string) bool {
	if storedHash == "" {
		return false
	}

	var alg, salt, prefix string
	parts := strings.SplitN(storedHash, "$", 3)
	switch len(parts) {
	case 3:
		alg = parts[0]
		salt = parts[1]
		prefix = parts[0] + "$" + parts[1] + "$"
	case 2:
		alg = parts[0]
		prefix = parts[0] + "$"
	default:
		alg = guessAlg(parts[0])
	}

	var digestBytes []byte
	if hmacKey != "" {
		hmacAlg := "sha512"
		keyPart := hmacKey
		if strings.Contains(hmacKey, ":") {
			keyParts := strings.SplitN(hmacKey, ":", 2)
			hmacAlg = keyParts[0]
			keyPart = keyParts[1]
		}

		secretBytes := []byte(keyPart + salt)
		textBytes := []byte(password)
		switch hmacAlg {
		case "sha512":
			digestBytes = toHMACSHA512(secretBytes, textBytes)
		default:
			return false
		}
	} else {
		text := password + salt
		textBytes := []byte(text)
		switch alg {
		case "sha512":
			digestBytes = toSHA512(textBytes)
		case "sha384":
			digestBytes = toSHA384(textBytes)
		case "sha256":
			digestBytes = toSHA256(textBytes)
		case "sha224":
			digestBytes = toSHA224(textBytes)
		case "sha1":
			digestBytes = toSHA1(textBytes)
		case "md5":
			digestBytes = toMD5(textBytes)
		default:
			return false
		}
	}

	computedHash := prefix + hex.EncodeToString(digestBytes)
	return hmac.Equal([]byte(computedHash), []byte(storedHash))
}
