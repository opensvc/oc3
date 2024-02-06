package handlers

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth/strategies/union"
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
			c.Set("user", user)
			return next(c)
		}
	}
}
