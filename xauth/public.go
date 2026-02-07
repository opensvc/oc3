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
		path   []string
		prefix []string
	}
)

var (
	ErrPrivatePath = errors.New("not public url")
)

func NewPublicStrategy(path, prefix []string) auth.Strategy {
	return &public{path: path, prefix: prefix}
}

func (p *public) Authenticate(_ context.Context, r *http.Request) (auth.Info, error) {
	uri := r.RequestURI
	for _, s := range p.path {
		if uri == s {
			return auth.NewUserInfo("public", "", nil, nil), nil
		}
	}
	for _, s := range p.prefix {
		if strings.HasPrefix(r.RequestURI, s) {
			return auth.NewUserInfo("public", "", nil, nil), nil
		}
	}
	return nil, ErrPrivatePath
}
