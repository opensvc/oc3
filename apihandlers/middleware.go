package apihandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth"
	"github.com/shaj13/go-guardian/v2/auth/strategies/union"

	"github.com/opensvc/oc3/xauth"
)

const (
	XNodeID = "XNodeID"
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
			if nodeID := ext.Get(xauth.XNodeID); nodeID != "" {
				// request user is a node, sets node ID in echo context
				c.Set(XNodeID, nodeID)
			}
			c.Set("user", user)
			return next(c)
		}
	}
}

// nodeIDFromContext returns the nodeID from context or zero string
// if not found.
func nodeIDFromContext(c echo.Context) string {
	user, ok := c.Get(XNodeID).(string)
	if ok {
		return user
	}
	return ""
}

func userInfoFromContext(c echo.Context) auth.Info {
	user, ok := c.Get("user").(auth.Info)
	if ok {
		return user
	}
	return nil
}
