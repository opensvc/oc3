package apicollectorhandlers

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/apicollector"
)

func JSONProblemf(ctx echo.Context, code int, title, detail string, args ...any) error {
	return JSONProblem(ctx, code, title, fmt.Sprintf(detail, args...))
}

func JSONProblem(ctx echo.Context, code int, title, detail string) error {
	if title == "" {
		title = http.StatusText(code)
	}
	return ctx.JSON(code, apicollector.Problem{
		Detail: detail,
		Title:  title,
		Status: code,
	})
}

func JSONNodeAuthProblem(c echo.Context) error {
	return JSONProblem(c, http.StatusForbidden, "", "expecting node credentials")
}
