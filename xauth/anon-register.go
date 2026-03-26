package xauth

import (
	"context"
	"net/http"

	"github.com/shaj13/go-guardian/v2/auth"
	"github.com/spf13/viper"
)

const (
	XAuthMode            = "auth_mode"
	AuthModeAnonRegister = "anon_register"
)

type anonRegister struct{}

func NewAnonRegister() auth.Strategy {
	return &anonRegister{}
}

func (a *anonRegister) Authenticate(_ context.Context, r *http.Request) (auth.Info, error) {
	if r.Method != http.MethodPost {
		return nil, ErrPrivatePath
	}
	if r.URL.Path != "/api/auth/node" {
		return nil, ErrPrivatePath
	}
	if !viper.GetBool("server.allow_anon_register") {
		return nil, ErrPrivatePath
	}
	if r.Header.Get("Authorization") != "" {
		return nil, ErrPrivatePath
	}

	ext := make(auth.Extensions)
	ext.Set(XAuthMode, AuthModeAnonRegister)
	return auth.NewUserInfo("anon-register", "", nil, ext), nil
}
