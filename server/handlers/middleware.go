package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth"
	"github.com/shaj13/go-guardian/v2/auth/strategies/union"

	"github.com/opensvc/oc3/xauth"
)

const (
	XClusterID = "XClusterID"
	XNodeID    = "XNodeID"
	XNodename  = "XNodename"
	XApp       = "XApp"
	XUserID    = "XUserID"
	XUserEmail = "XUserEmail"
	XAuthMode  = "XAuthMode" // user or node
)

const (
	AuthModeUser = "user"
	AuthModeNode = "node"
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
				c.Set(XNodeID, nodeID)
				c.Set(XAuthMode, AuthModeNode)
			}
			if nodename := ext.Get(xauth.XNodename); nodename != "" {
				c.Set(XNodename, nodename)
			}
			if clusterID := ext.Get(xauth.XClusterID); clusterID != "" {
				c.Set(XClusterID, clusterID)
			}
			if app := ext.Get(xauth.XApp); app != "" {
				c.Set(XApp, app)
			}
			if userEmail := ext.Get(xauth.XUserEmail); userEmail != "" {
				c.Set(XUserEmail, userEmail)
				c.Set(XAuthMode, AuthModeUser)
			}

			groups := user.GetGroups()
			c.Set("groups", groups)
			c.Set("user", user)

			return next(c)
		}
	}
}

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

// return true if the request is authenticated as a node
func IsAuthByNode(c echo.Context) bool {
	authMode, ok := c.Get(XAuthMode).(string)
	return ok && authMode == AuthModeNode
}
