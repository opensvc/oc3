package xauth

import (
	"errors"

	"github.com/shaj13/go-guardian/v2/auth/strategies/union"
)

// ErrUnavailable is returned by the strategies that could not check the
// credentials, the database being unreachable or failing: the request is to be
// answered as a service failure, not as wrong credentials.
var ErrUnavailable = errors.New("authentication unavailable")

// IsUnavailable reports whether the error of a strategy, or of one of the
// strategies of a union, is ErrUnavailable.
func IsUnavailable(err error) bool {
	var errs union.MultiError
	if errors.As(err, &errs) {
		for _, e := range errs {
			if errors.Is(e, ErrUnavailable) {
				return true
			}
		}
		return false
	}
	return errors.Is(err, ErrUnavailable)
}
