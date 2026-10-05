package xauth

import (
	"errors"
	"fmt"
	"testing"

	"github.com/shaj13/go-guardian/v2/auth/strategies/union"
)

func TestIsUnavailable(t *testing.T) {
	wrapped := fmt.Errorf("%w: %w", ErrUnavailable, errors.New("dial tcp: connection refused"))
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"wrong credentials", errors.New("invalid credentials"), false},
		{"database failure", wrapped, true},
		{"union of wrong credentials", union.MultiError{errors.New("not public url"), errors.New("invalid credentials")}, false},
		{"union with a database failure", union.MultiError{errors.New("not public url"), wrapped, errors.New("invalid credentials")}, true},
	}
	for _, c := range cases {
		if got := IsUnavailable(c.err); got != c.want {
			t.Errorf("%s: IsUnavailable = %v, want %v", c.name, got, c.want)
		}
	}
}
