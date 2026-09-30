package serverhandlers

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/logkey"
)

// errRequestDenied is returned by the require* guards after they have written the
// problem response. JSONProblemf returns nil once the response is written, so a
// guard returning its result would let the caller go on with the denied action:
// callers stop on a non-nil error, and echo's error handler leaves the already
// committed response untouched.
var errRequestDenied = errors.New("request denied, problem response sent")

// denyRequest writes the problem response and returns errRequestDenied, or the
// error that prevented writing the response.
func denyRequest(ctx echo.Context, code int, format string, args ...any) error {
	if err := JSONProblemf(ctx, code, format, args...); err != nil {
		return err
	}
	return errRequestDenied
}

func JSONProblemf(ctx echo.Context, code int, format string, args ...any) error {
	return JSONProblem(ctx, code, fmt.Sprintf(format, args...))
}

func JSONProblem(ctx echo.Context, code int, s string) error {
	return ctx.JSON(code, server.Problem{Text: s})
}

func JSONNodeAuthProblem(c echo.Context) error {
	return JSONProblem(c, http.StatusForbidden, "expecting node credentials")
}

// stringPtr converts a pointer to a generated enum string type to a *string,
// keeping nil for an absent property.
func stringPtr[T ~string](value *T) *string {
	if value == nil {
		return nil
	}
	s := string(*value)
	return &s
}

// httpError is a request refused with an HTTP status, the way the historical
// collector raised HTTP(status, message): the handlers return it, and write it
// once with httpProblem.
type httpError struct {
	status int
	msg    string
}

func (e *httpError) Error() string { return e.msg }

func httpErrorf(status int, format string, args ...any) error {
	return &httpError{status: status, msg: fmt.Sprintf(format, args...)}
}

// httpProblem writes the problem response of a refused request: its status for
// an httpError, 500 otherwise.
func httpProblem(c echo.Context, err error) error {
	var he *httpError
	if errors.As(err, &he) {
		return JSONProblem(c, he.status, he.msg)
	}
	return JSONProblem(c, http.StatusInternalServerError, err.Error())
}

// httpInternal logs an internal failure and turns it into a 500 with a
// message that does not leak the database error.
func httpInternal(log *slog.Logger, msg string, err error) error {
	log.Error(msg, logkey.Error, err)
	return httpErrorf(http.StatusInternalServerError, "%s", msg)
}
