package handlers

import (
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/api"
)

func JSONProblem(ctx echo.Context, code int, title, detail string) error {
	return ctx.JSON(code, api.Problem{
		Detail: detail,
		Title:  title,
		Status: code,
	})
}
