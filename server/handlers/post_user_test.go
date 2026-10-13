package serverhandlers

import (
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth"

	"github.com/opensvc/oc3/xauth"
)

func TestIdentityManagedByProvider(t *testing.T) {
	ctx := func(source string, impersonating bool) echo.Context {
		c := echo.New().NewContext(httptest.NewRequest("POST", "/users/self", nil), httptest.NewRecorder())
		if source != "" {
			ext := make(auth.Extensions)
			ext.Set(xauth.XAuthSource, source)
			c.Set("user", auth.NewUserInfo("u@example.com", "1", nil, ext))
		}
		if impersonating {
			c.Set(XImpersonator, auth.NewUserInfo("m@example.com", "2", nil, nil))
		}
		return c
	}
	cases := []struct {
		name          string
		source        string
		impersonating bool
		want          bool
	}{
		{"oidc session", xauth.AuthSourceSession, false, true},
		{"bearer token", xauth.AuthSourceBearer, false, true},
		{"password", xauth.AuthSourceBasic, false, false},
		{"no user", "", false, false},
		{"oidc manager acting as another user", xauth.AuthSourceSession, true, false},
	}
	for _, tc := range cases {
		if got := identityManagedByProvider(ctx(tc.source, tc.impersonating)); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}
