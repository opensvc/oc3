package serverhandlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
)

// checkCompManager refuses a caller without the CompManager privilege, as
// check_privilege("CompManager") and @auth.requires_membership('CompManager')
// do. Unlike requireCompManager, it returns the refusal as an httpError rather
// than writing it.
func checkCompManager(c echo.Context) error {
	if !IsAuthByUser(c) {
		return httpErrorf(http.StatusUnauthorized, "user authentication required")
	}
	if !IsCompManager(c) {
		return httpErrorf(http.StatusForbidden, "CompManager privilege required")
	}
	return nil
}

// compDesignerError turns the errors of the compliance designer queries into
// HTTP statuses: 404 for a missing object, 409 for a name already taken, 500
// otherwise.
func compDesignerError(c echo.Context, name string, err error) error {
	switch {
	case errors.Is(err, cdb.ErrCompNotFound):
		return httpErrorf(http.StatusNotFound, "%s", err.Error())
	case errors.Is(err, cdb.ErrCompConflict):
		return httpErrorf(http.StatusConflict, "%s", strings.TrimPrefix(err.Error(), cdb.ErrCompConflict.Error()+": "))
	}
	return httpInternal(echolog.GetLogHandler(c, name), "cannot "+name, err)
}

// resolveRuleset returns the id of a ruleset given by id or by name: 404 when
// there is none.
func (a *Api) resolveRuleset(ctx context.Context, idOrName string) (int64, error) {
	id, found, err := a.ODB.CompRulesetID(ctx, idOrName)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, httpErrorf(http.StatusNotFound, "ruleset %s not found", idOrName)
	}
	return id, nil
}

// compBool reads a boolean property the way the historical collector accepted
// it, a JSON boolean or "T", "F", "true", "false", "yes", "no", "1", "0", and
// stores it as its "T" or "F" varchar(1).
func compBool(key string, v any) (string, error) {
	switch t := v.(type) {
	case bool:
		if t {
			return "T", nil
		}
		return "F", nil
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "t", "true", "yes", "y", "1":
			return "T", nil
		case "f", "false", "no", "n", "0":
			return "F", nil
		}
	case float64:
		if t == 1 {
			return "T", nil
		} else if t == 0 {
			return "F", nil
		}
	}
	return "", httpErrorf(http.StatusBadRequest, "invalid %s value %v: a boolean is expected", key, v)
}

// compString reads a string property, trimmed; an empty one is refused when
// required.
func compString(key string, v any, required bool) (string, error) {
	s, ok := v.(string)
	if !ok {
		if v == nil && !required {
			return "", nil
		}
		return "", httpErrorf(http.StatusBadRequest, "invalid %s value %v: a string is expected", key, v)
	}
	s = strings.TrimSpace(s)
	if s == "" && required {
		return "", httpErrorf(http.StatusBadRequest, "the '%s' key is mandatory", key)
	}
	return s, nil
}

// compOneOf checks a value against the accepted ones.
func compOneOf(key, v string, accepted ...string) error {
	for _, a := range accepted {
		if v == a {
			return nil
		}
	}
	return httpErrorf(http.StatusBadRequest, "invalid %s value %q: expected one of %s", key, v, strings.Join(accepted, ", "))
}

// compInfo answers with an informational message, as the historical handlers'
// dict(info=...).
func compInfo(c echo.Context, format string, args ...any) error {
	return c.JSON(http.StatusOK, map[string]string{"info": fmt.Sprintf(format, args...)})
}

// compJSON renders the properties of a change for the log, as the historical
// collector logs str(vars).
func compJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}
