package handlers

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

func (a *Api) PostDaemonStatus(c echo.Context) error {
	nodeID := nodeIDFromContext(c)
	if nodeID == "" {
		return JSONNodeAuthProblem(c)
	}

	return JSONProblem(c, http.StatusInternalServerError, "not yet implemented", "")
}
