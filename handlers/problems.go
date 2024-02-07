package handlers

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/api"
)

func JSONProblemf(ctx echo.Context, code int, title, detail string, args ...any) error {
	return JSONProblem(ctx, code, title, fmt.Sprintf(detail, args...))
}

func JSONProblem(ctx echo.Context, code int, title, detail string) error {
	if title == "" {
		title = http.StatusText(code)
	}
	return ctx.JSON(code, api.Problem{
		Detail: detail,
		Title:  title,
		Status: code,
	})
}

func JSONNodeAuthProblem(c echo.Context) error {
	return JSONProblem(c, http.StatusForbidden, "", "expecting node credentials")
}
