package serverhandlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth"

	"github.com/opensvc/oc3/xauth"
)

// TestPasswordsRefusedWithoutBasicUsers checks that, with password sign-in
// turned off, no password is set: neither at user creation nor on the profile.
// Both refusals come before the database is read.
func TestPasswordsRefusedWithoutBasicUsers(t *testing.T) {
	request := func(path, body string) (echo.Context, *httptest.ResponseRecorder) {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		rec := httptest.NewRecorder()
		c := echo.New().NewContext(req, rec)
		ext := make(auth.Extensions)
		ext.Set(xauth.XAuthSource, xauth.AuthSourceBasic)
		ext.Set(xauth.XUserID, "1")
		c.Set("user", auth.NewUserInfo("m@example.com", "1", nil, ext))
		c.Set(XAuthMode, AuthModeUser)
		c.Set("groups", []string{"Manager"})
		return c, rec
	}
	a := &Api{BasicUsers: false}

	c, rec := request("/users", `{"email": "new@example.com", "password": "a password"}`)
	if err := a.PostUsers(c); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "password sign-in is disabled") {
		t.Errorf("create with a password: got %d %s", rec.Code, rec.Body.String())
	}

	c, rec = request("/users/self/password", `{"current_password": "old password!", "new_password": "a new password"}`)
	if err := a.PostUserSelfPassword(c); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "password sign-in is disabled") {
		t.Errorf("change of one's password: got %d %s", rec.Code, rec.Body.String())
	}
}
