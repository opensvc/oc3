package serverhandlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"
	"gopkg.in/yaml.v3"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/logkey"
)

// formError is a forms request refused with an HTTP status, the way the
// historical collector raised HTTP(status, message).
type formError struct {
	status int
	msg    string
}

func (e *formError) Error() string { return e.msg }

func formErrorf(status int, format string, args ...any) error {
	return &formError{status: status, msg: fmt.Sprintf(format, args...)}
}

// formProblem writes the problem response of a forms request.
func formProblem(c echo.Context, err error) error {
	var fe *formError
	if errors.As(err, &fe) {
		return JSONProblem(c, fe.status, fe.msg)
	}
	return JSONProblem(c, http.StatusInternalServerError, err.Error())
}

// formInternal logs an internal failure and turns it into a 500 with a
// message that does not leak the database error.
func formInternal(log *slog.Logger, msg string, err error) error {
	log.Error(msg, logkey.Error, err)
	return formErrorf(http.StatusInternalServerError, "%s", msg)
}

// requireFormsManager refuses a caller without the FormsManager privilege, as
// check_privilege("FormsManager") does: a manager always passes.
func requireFormsManager(c echo.Context) error {
	if !IsAuthByUser(c) {
		return formErrorf(http.StatusUnauthorized, "user authentication required")
	}
	if !IsManager(c) && !HasGroup(c, "FormsManager") {
		return formErrorf(http.StatusForbidden, "user has no FormsManager privilege")
	}
	return nil
}

// formCaller is the authenticated user, as the forms record it.
type formCaller struct {
	id    int64
	name  string // "First Last", as user_name()
	email string
}

// author returns "First Last <email>", as user_name(email=True).
func (fc formCaller) author() string {
	if fc.email == "" {
		return fc.name
	}
	return fc.name + " <" + fc.email + ">"
}

func (a *Api) formCaller(ctx context.Context, c echo.Context) (formCaller, error) {
	id := authUserID(c)
	if id == nil {
		if nodeID, _ := c.Get(XNodeID).(string); nodeID != "" {
			return formCaller{name: "agent"}, nil
		}
		return formCaller{name: "Unknown"}, nil
	}
	name, email, err := a.ODB.UserName(ctx, *id)
	if err != nil {
		return formCaller{}, err
	}
	return formCaller{id: *id, name: name, email: email}, nil
}

// decodeEntries reads a JSON body holding one object or a list of objects, as
// the historical collector's handlers accept both. isList tells which. A
// form-encoded body, as the historical examples post (curl -d key=value), is
// read as one object of strings.
func decodeEntries(c echo.Context) (entries []map[string]any, isList bool, err error) {
	contentType := c.Request().Header.Get(echo.HeaderContentType)
	if strings.HasPrefix(contentType, echo.MIMEApplicationForm) || strings.HasPrefix(contentType, echo.MIMEMultipartForm) {
		values, err := c.FormParams()
		if err != nil {
			return nil, false, formErrorf(http.StatusBadRequest, "invalid request body: %s", err)
		}
		entry := make(map[string]any, len(values))
		for k, v := range values {
			if len(v) > 0 {
				entry[k] = v[0]
			}
		}
		return []map[string]any{entry}, false, nil
	}
	var raw json.RawMessage
	if err := json.NewDecoder(c.Request().Body).Decode(&raw); err != nil {
		return nil, false, formErrorf(http.StatusBadRequest, "invalid request body: %s", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if trimmed := bytes.TrimSpace(raw); len(trimmed) > 0 && trimmed[0] == '[' {
		if err := decoder.Decode(&entries); err != nil {
			return nil, true, formErrorf(http.StatusBadRequest, "invalid request body: %s", err)
		}
		return entries, true, nil
	}
	var entry map[string]any
	if err := decoder.Decode(&entry); err != nil {
		return nil, false, formErrorf(http.StatusBadRequest, "invalid request body: %s", err)
	}
	return []map[string]any{entry}, false, nil
}

// entryString reads a key of a decoded entry as a string, as posted form
// variables are.
func entryString(entry map[string]any, key string) (string, bool) {
	v, ok := entry[key]
	if !ok || v == nil {
		return "", ok
	}
	switch t := v.(type) {
	case string:
		return t, true
	case json.Number:
		return t.String(), true
	}
	return fmt.Sprint(v), true
}

// runEntries answers a request whose body is one entry or a list. One entry
// answers with its result or its problem; a list always answers 200 and merges
// the info, error and data keys of every entry, as handle_list() does.
func runEntries(c echo.Context, entries []map[string]any, isList bool, run func(entry map[string]any) (map[string]any, error)) error {
	if !isList {
		result, err := run(entries[0])
		if err != nil {
			return formProblem(c, err)
		}
		return c.JSON(http.StatusOK, result)
	}
	merged := map[string][]any{"info": {}, "error": {}, "data": {}}
	for _, entry := range entries {
		result, err := run(entry)
		if err != nil {
			status := http.StatusInternalServerError
			var fe *formError
			if errors.As(err, &fe) {
				status = fe.status
			}
			merged["error"] = append(merged["error"], fmt.Sprintf("%d %s: %s", status, http.StatusText(status), err.Error()))
			continue
		}
		for _, key := range []string{"info", "error", "data"} {
			value, ok := result[key]
			if !ok {
				continue
			}
			switch v := value.(type) {
			case []any:
				merged[key] = append(merged[key], v...)
			case []map[string]any:
				for _, item := range v {
					merged[key] = append(merged[key], item)
				}
			case []string:
				for _, item := range v {
					merged[key] = append(merged[key], item)
				}
			default:
				merged[key] = append(merged[key], v)
			}
		}
	}
	return c.JSON(http.StatusOK, merged)
}

// parseFormYaml parses a form definition into JSON-ready values.
func parseFormYaml(s string) (any, error) {
	var v any
	if err := yaml.Unmarshal([]byte(s), &v); err != nil {
		return nil, err
	}
	return jsonReady(v), nil
}

// jsonReady converts the maps yaml decodes with non-string keys, which JSON
// cannot encode, keying them by their text.
func jsonReady(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, item := range t {
			t[k] = jsonReady(item)
		}
		return t
	case map[any]any:
		out := make(map[string]any, len(t))
		for k, item := range t {
			out[fmt.Sprint(k)] = jsonReady(item)
		}
		return out
	case []any:
		for i, item := range t {
			t[i] = jsonReady(item)
		}
		return t
	}
	return v
}

// formDefinitionProp computes form_definition from form_yaml, left out when the
// yaml does not parse, as mangle_form_data() does.
var formDefinitionProp = map[string]virtualProp{
	"form_definition": {
		requires: []string{"form_yaml"},
		compute: func(item map[string]any) (any, bool) {
			s, _ := item["form_yaml"].(string)
			v, err := parseFormYaml(s)
			if err != nil {
				return nil, false
			}
			return v, true
		},
	},
}

// parseFormID reads a form id given as a path segment or a body key.
func parseFormID(s string) (int64, error) {
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, formErrorf(http.StatusBadRequest, "invalid form id %q", s)
	}
	return id, nil
}

// requireFormResponsible refuses a caller not responsible for the form, as
// form_responsible() does, with its 404.
func (a *Api) requireFormResponsible(c echo.Context, log *slog.Logger, ctx context.Context, formID int64) error {
	ok, err := a.ODB.FormResponsible(ctx, formID, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		return formInternal(log, "cannot check form responsibility", err)
	}
	if !ok {
		return formErrorf(http.StatusNotFound, "Form %d not found or you are not responsible", formID)
	}
	return nil
}

// requireFormPublished refuses a caller the form is not published to, as
// form_published() does, with its 404.
func (a *Api) requireFormPublished(c echo.Context, log *slog.Logger, ctx context.Context, formID int64) error {
	ok, err := a.ODB.FormVisible(ctx, formID, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		return formInternal(log, "cannot check form publication", err)
	}
	if !ok {
		return formErrorf(http.StatusNotFound, "Form %d not found or not published to you", formID)
	}
	return nil
}

// formLog writes an audit log entry of a forms change.
func (a *Api) formLog(ctx context.Context, c echo.Context, log *slog.Logger, action, format string, dict map[string]any) {
	userEmail, _ := c.Get(XUserEmail).(string)
	if err := a.ODB.Log(ctx, cdb.LogEntry{Action: action, User: userEmail, Fmt: format, Dict: dict, Level: "info"}); err != nil {
		log.Error("cannot write audit log", logkey.Error, err)
	}
}

// formNotify publishes the changes of a forms request to the messenger.
func (a *Api) formNotify(ctx context.Context, log *slog.Logger) {
	if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}
}

// pyFormat renders a historical collector message, "%(key)s" placeholders
// filled from dict, for the info messages the answers carry.
func pyFormat(format string, dict map[string]any) string {
	out := format
	for k, v := range dict {
		out = strings.ReplaceAll(out, "%("+k+")s", fmt.Sprint(v))
		out = strings.ReplaceAll(out, "%("+k+")d", fmt.Sprint(v))
	}
	return out
}
