package serverhandlers

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
)

func JSONProblemf(ctx echo.Context, code int, format string, args ...any) error {
	return JSONProblem(ctx, code, fmt.Sprintf(format, args...))
}

func JSONProblem(ctx echo.Context, code int, s string) error {
	return ctx.JSON(code, server.Problem{Text: s})
}

func JSONNodeAuthProblem(c echo.Context) error {
	return JSONProblem(c, http.StatusForbidden, "expecting node credentials")
}
