package xauth

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/shaj13/go-guardian/v2/auth"
)

type (
	public struct {
		prefix []string
	}
)

var (
	ErrPrivatePath = errors.New("not public url")
)

func NewPublicStrategy(s ...string) auth.Strategy {
	return &public{prefix: s}
}

func (p *public) Authenticate(_ context.Context, r *http.Request) (auth.Info, error) {
	for _, s := range p.prefix {
		if strings.HasPrefix(r.RequestURI, s) {
			return auth.NewUserInfo("public", "", nil, nil), nil
		}
	}
	return nil, ErrPrivatePath
}
