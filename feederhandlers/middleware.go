package feederhandlers

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

				if nodename := ext.Get(xauth.XNodename); nodename != "" {
					c.Set(XNodename, nodename)
				}

				if clusterID := ext.Get(xauth.XClusterID); clusterID != "" {
					// request user is a node with a cluster ID, sets cluster ID in echo context
					c.Set(XClusterID, clusterID)
				}
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

// nodenameFromContext returns the nodename from context or zero string
// if not found.
func nodenameFromContext(c echo.Context) string {
	nodename, ok := c.Get(XNodename).(string)
	if ok {
		return nodename
	}
	return ""
}

// clusterIDFromContext returns the clusterID from context or zero string
// if not found.
func clusterIDFromContext(c echo.Context) string {
	s, ok := c.Get(XClusterID).(string)
	if ok {
		return s
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
