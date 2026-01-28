package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth"
	"github.com/shaj13/go-guardian/v2/auth/strategies/union"

	"github.com/opensvc/oc3/xauth"
)

const (
	XUserID    = "XUserID"
	XUserEmail = "XUserEmail"
)

// AuthMiddleware returns auth middleware that authenticate requests from strategies.
func AuthMiddleware(strategies union.Union) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			_, user, err := strategies.AuthenticateRequest(c.Request())

			if err != nil {
				code := http.StatusUnauthorized
				return JSONProblem(c, code, http.StatusText(code), err.Error())
			}

			ext := user.GetExtensions()
			if userEmail := ext.Get(xauth.XUserEmail); userEmail != "" {
				c.Set(XUserEmail, userEmail)
			}

			groups := user.GetGroups()
			c.Set("groups", groups)
			c.Set("user", user)

			return next(c)
		}
	}
}

// // userEmailFromContext returns the userEmail from context
// func userEmailFromContext(c echo.Context) string {
// 	user, ok := c.Get(XUserEmail).(string)
// 	if ok {
// 		return user
// 	}
// 	return ""
// }

func UserInfoFromContext(c echo.Context) auth.Info {
	user, ok := c.Get("user").(auth.Info)
	if ok {
		return user
	}
	return nil
}

func UserGroupsFromContext(c echo.Context) []string {
	groups, ok := c.Get("groups").([]string)
	if ok {
		return groups
	}
	return nil
}

func IsManager(c echo.Context) bool {
	groups := UserGroupsFromContext(c)
	for _, g := range groups {
		if g == "Manager" {
			return true
		}
	}
	return false
}
