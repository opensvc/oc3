package serverhandlers

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// constrainableNodeFields are the node attributes whose values may be limited to a
// list: the text attributes a user sets (UpdateNodeFields), whose values repeat
// from one node to the next. Left out are the teams and the app, already chosen
// among known ones, the dates, numbers and booleans, and action_type, a fixed
// enumeration.
var constrainableNodeFields = []string{
	"asset_env", "sec_zone", "status", "role", "type",
	"tz", "loc_country", "loc_city", "loc_zip", "loc_addr",
	"loc_building", "loc_floor", "loc_room", "loc_rack", "enclosure",
	"hv", "hvpool", "hvvdc",
	"power_cabinet1", "power_cabinet2", "power_protect", "power_protect_breaker",
	"power_breaker1", "power_breaker2",
}

// constrainableFields are, per kind of object, the text attributes a user sets
// whose values may be constrained, in display order. A service: its disaster
// recovery nodes (its app is chosen among the known ones, its wave and SLA are
// numbers). An app: its domain (its code is its name, its team chosen among the
// known ones, its description free).
var constrainableFields = map[server.FieldConstraintObjectKind][]string{
	server.FieldConstraintObjectKindNode:    constrainableNodeFields,
	server.FieldConstraintObjectKindService: {"svc_drpnode", "svc_drpnodes"},
	server.FieldConstraintObjectKindApp:     {"app_domain"},
}

// constrainedTables name, per kind of object, its table and the column of its id,
// to read the values an object holds.
var constrainedTables = map[server.FieldConstraintObjectKind][2]string{
	server.FieldConstraintObjectKindNode:    {"nodes", "node_id"},
	server.FieldConstraintObjectKindService: {"services", "svc_id"},
	server.FieldConstraintObjectKindApp:     {"apps", "id"},
}

const (
	fieldConstraintMaxValues  = 2000
	fieldConstraintMaxLength  = 255
	fieldConstraintMaxPattern = 1024
)

// compilePattern compiles the pattern of a regex constraint, anchored: the whole
// value must match it, not a part.
func compilePattern(pattern string) (*regexp.Regexp, error) {
	return regexp.Compile("^(?:" + pattern + ")$")
}

func isConstrainable(kind server.FieldConstraintObjectKind, field string) bool {
	for _, f := range constrainableFields[kind] {
		if f == field {
			return true
		}
	}
	return false
}

// fieldConstraintList returns the constrained attributes of a kind of object, in
// the order of constrainableFields; the others are free text, the default, and
// absent. A stored row for an attribute no longer constrainable is left out.
func fieldConstraintList(kind server.FieldConstraintObjectKind, stored []cdb.FieldConstraint) server.FieldConstraintListResponse {
	byField := make(map[string]cdb.FieldConstraint, len(stored))
	for _, c := range stored {
		byField[c.Field] = c
	}
	fields := constrainableFields[kind]
	if fields == nil {
		fields = []string{}
	}
	out := server.FieldConstraintListResponse{Data: []server.FieldConstraint{}, Fields: fields}
	for _, field := range fields {
		c, ok := byField[field]
		if !ok {
			continue
		}
		out.Data = append(out.Data, server.FieldConstraint{
			ObjectKind: kind, Field: field, Mode: server.FieldConstraintMode(c.Mode), Values: c.Values,
			Pattern: c.Pattern, Updated: c.Updated.Format(time.RFC3339), UpdatedBy: c.UpdatedBy,
		})
	}
	return out
}

// GetFieldConstraints handles GET /field_constraints/{object_kind}.
func (a *Api) GetFieldConstraints(c echo.Context, objectKind server.FieldConstraintObjectKind) error {
	log := echolog.GetLogHandler(c, "GetFieldConstraints")
	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	if !objectKind.Valid() {
		return JSONProblemf(c, http.StatusBadRequest, "unknown object_kind %q", objectKind)
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	stored, err := a.ODB.FieldConstraints(ctx, string(objectKind))
	if err != nil {
		log.Error("cannot read the field constraints", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot read the field constraints")
	}
	return c.JSON(http.StatusOK, fieldConstraintList(objectKind, stored))
}

// GetFieldConstraintList handles GET /field_constraints: the constraints of every
// kind of object as a list, filtered, sorted and paged like the others.
func (a *Api) GetFieldConstraintList(c echo.Context, params server.GetFieldConstraintListParams) error {
	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	return a.handleList(c, "GetFieldConstraintList", "field_constraint", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetFieldConstraintList(ctx, p)
	})
}

// cleanConstraint checks a constraint as sent by the client and returns what to
// store: in mode regex, the pattern, which must compile, and no value; in the
// list modes, the values trimmed, the empty ones and the duplicates dropped, the
// order kept, and no pattern.
func cleanConstraint(mode server.FieldConstraintMode, raw []string, pattern *string) ([]string, string, error) {
	if !mode.Valid() {
		return nil, "", fmt.Errorf("unknown mode %q", mode)
	}
	if mode == server.FieldConstraintModeRegex {
		p := ""
		if pattern != nil {
			p = strings.TrimSpace(*pattern)
		}
		switch {
		case p == "":
			return nil, "", fmt.Errorf("mode regex needs a pattern")
		case utf8.RuneCountInString(p) > fieldConstraintMaxPattern:
			return nil, "", fmt.Errorf("the pattern is longer than %d characters", fieldConstraintMaxPattern)
		}
		// Compiled as typed first, for the error to quote the pattern sent, not
		// its anchored form.
		if _, err := regexp.Compile(p); err != nil {
			return nil, "", fmt.Errorf("invalid pattern: %w", err)
		}
		if _, err := compilePattern(p); err != nil {
			return nil, "", fmt.Errorf("invalid pattern: %w", err)
		}
		return []string{}, p, nil
	}
	values, err := cleanConstraintValues(raw)
	return values, "", err
}

// cleanConstraintValues trims the values, drops the empty ones and the
// duplicates, keeps the order, and checks the limits.
func cleanConstraintValues(raw []string) ([]string, error) {
	values := []string{}
	seen := map[string]bool{}
	for _, v := range raw {
		v = strings.TrimSpace(v)
		if v == "" || seen[v] {
			continue
		}
		if utf8.RuneCountInString(v) > fieldConstraintMaxLength {
			return nil, fmt.Errorf("the value %q is longer than %d characters", v, fieldConstraintMaxLength)
		}
		seen[v] = true
		values = append(values, v)
	}
	if len(values) > fieldConstraintMaxValues {
		return nil, fmt.Errorf("more than %d values", fieldConstraintMaxValues)
	}
	if len(values) == 0 {
		// No list is free text, the default: the constraint is removed instead.
		return nil, fmt.Errorf("a constraint needs at least one value")
	}
	return values, nil
}

// PostFieldConstraint handles POST /field_constraints/{object_kind}/{field}.
func (a *Api) PostFieldConstraint(c echo.Context, objectKind server.FieldConstraintObjectKind, field string) error {
	log := echolog.GetLogHandler(c, "PostFieldConstraint")
	if err := requireManager(c); err != nil {
		return err
	}
	if !objectKind.Valid() {
		return JSONProblemf(c, http.StatusBadRequest, "unknown object_kind %q", objectKind)
	}
	if !isConstrainable(objectKind, field) {
		return JSONProblemf(c, http.StatusNotFound, "the %s attribute %s cannot be constrained", objectKind, field)
	}
	var body server.PostFieldConstraintJSONRequestBody
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	var raw []string
	if body.Values != nil {
		raw = *body.Values
	}
	values, pattern, err := cleanConstraint(body.Mode, raw, body.Pattern)
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	author, _ := c.Get(XUserEmail).(string)
	if err := a.inTx(ctx, log, func(tx *cdb.DB) error {
		if err := tx.SetFieldConstraint(ctx, cdb.FieldConstraint{
			ObjectKind: string(objectKind), Field: field, Mode: string(body.Mode),
			Values: values, Pattern: pattern, UpdatedBy: author,
		}); err != nil {
			return err
		}
		entry := cdb.LogEntry{Action: "field_constraint.change", User: author, Level: "info",
			Fmt: "limited the %(kind)s attribute %(field)s to %(count)s values (%(mode)s): %(values)s",
			Dict: map[string]any{"kind": string(objectKind), "field": field, "mode": string(body.Mode),
				"count": len(values), "values": auditValues(values)},
		}
		if body.Mode == server.FieldConstraintModeRegex {
			entry.Fmt = "limited the %(kind)s attribute %(field)s to the values matching %(pattern)s"
			entry.Dict = map[string]any{"kind": string(objectKind), "field": field, "mode": string(body.Mode), "pattern": pattern}
		}
		if err := tx.Log(ctx, entry); err != nil {
			log.Error("cannot write audit log", logkey.Error, err)
		}
		return nil
	}); err != nil {
		log.Error("cannot change the field constraint", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot change the field constraint")
	}
	if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}
	stored, err := a.ODB.FieldConstraints(ctx, string(objectKind))
	if err != nil {
		log.Error("cannot read the field constraints", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot read the field constraints")
	}
	list := fieldConstraintList(objectKind, stored)
	for _, entry := range list.Data {
		if entry.Field == field {
			return c.JSON(http.StatusOK, server.FieldConstraintListResponse{Data: []server.FieldConstraint{entry}, Fields: list.Fields})
		}
	}
	return JSONProblem(c, http.StatusInternalServerError, "cannot read the field constraint")
}

// DeleteFieldConstraint handles DELETE /field_constraints/{object_kind}/{field}:
// the attribute becomes free text again, the default, and its row goes.
func (a *Api) DeleteFieldConstraint(c echo.Context, objectKind server.FieldConstraintObjectKind, field string) error {
	log := echolog.GetLogHandler(c, "DeleteFieldConstraint")
	if err := requireManager(c); err != nil {
		return err
	}
	if !objectKind.Valid() {
		return JSONProblemf(c, http.StatusBadRequest, "unknown object_kind %q", objectKind)
	}
	if !isConstrainable(objectKind, field) {
		return JSONProblemf(c, http.StatusNotFound, "the %s attribute %s cannot be constrained", objectKind, field)
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	author, _ := c.Get(XUserEmail).(string)
	var removed bool
	if err := a.inTx(ctx, log, func(tx *cdb.DB) error {
		var err error
		if removed, err = tx.DeleteFieldConstraint(ctx, string(objectKind), field); err != nil || !removed {
			return err
		}
		if err := tx.Log(ctx, cdb.LogEntry{Action: "field_constraint.delete", User: author, Level: "info",
			Fmt:  "made the %(kind)s attribute %(field)s free text again",
			Dict: map[string]any{"kind": string(objectKind), "field": field},
		}); err != nil {
			log.Error("cannot write audit log", logkey.Error, err)
		}
		return nil
	}); err != nil {
		log.Error("cannot remove the field constraint", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot remove the field constraint")
	}
	if !removed {
		return JSONProblemf(c, http.StatusNotFound, "the %s attribute %s has no constraint", objectKind, field)
	}
	if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}
	return JSONProblemf(c, http.StatusOK, "the %s attribute %s is free text again", objectKind, field)
}

// enforced tells whether the api refuses the values a constraint does not allow:
// strict and regex do, suggest only offers its values.
func enforced(mode string) bool {
	return mode == string(server.FieldConstraintModeStrict) || mode == string(server.FieldConstraintModeRegex)
}

// strictViolation returns why the values set break a strict or regex constraint,
// empty when they do not: a value must be one of the list, or match the whole
// pattern, or be empty, or be the value the object holds (current), so that an
// object holding a value from before the constraint can still be saved.
func strictViolation(constraints []cdb.FieldConstraint, fields map[string]any, current map[string]string) string {
	for _, c := range constraints {
		if !enforced(c.Mode) {
			continue
		}
		raw, ok := fields[c.Field]
		if !ok {
			continue
		}
		value, ok := raw.(string)
		if !ok {
			continue
		}
		if value == "" || value == current[c.Field] {
			continue
		}
		if c.Mode == string(server.FieldConstraintModeRegex) {
			re, err := compilePattern(c.Pattern)
			// A stored pattern that no longer compiles refuses rather than lets through.
			if err != nil || !re.MatchString(value) {
				return fmt.Sprintf("%s: %q does not match the pattern %s", c.Field, value, c.Pattern)
			}
			continue
		}
		allowed := false
		for _, v := range c.Values {
			if v == value {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Sprintf("%s: %q is not one of the allowed values", c.Field, value)
		}
	}
	return ""
}

// checkConstraints refuses the values a user sets on an object that the strict
// and regex constraints of its kind do not allow. id is empty for an object being
// created. The writes of the nodes are not checked: they report what the node
// has.
func checkConstraints(c echo.Context, ctx context.Context, odb *cdb.DB, kind server.FieldConstraintObjectKind, id string, fields map[string]any) (string, error) {
	if !IsAuthByUser(c) {
		return "", nil
	}
	constraints, err := odb.FieldConstraints(ctx, string(kind))
	if err != nil {
		return "", err
	}
	var strict []string
	for _, k := range constraints {
		if _, set := fields[k.Field]; set && enforced(k.Mode) && isConstrainable(kind, k.Field) {
			strict = append(strict, k.Field)
		}
	}
	if len(strict) == 0 {
		return "", nil
	}
	current := map[string]string{}
	if id != "" {
		table := constrainedTables[kind]
		if current, err = odb.ObjectStringValues(ctx, table[0], table[1], id, strict); err != nil {
			return "", err
		}
	}
	return strictViolation(constraints, fields, current), nil
}

// checkNodeConstraints is checkConstraints for a node.
func checkNodeConstraints(c echo.Context, ctx context.Context, odb *cdb.DB, nodeID string, fields map[string]any) (string, error) {
	return checkConstraints(c, ctx, odb, server.FieldConstraintObjectKindNode, nodeID, fields)
}

// auditValues lists the values for the audit log, cut short past 1000 characters:
// the log entry is a text column, and a list may hold 2000 values.
func auditValues(values []string) string {
	const limit = 1000
	text := strings.Join(values, ", ")
	if utf8.RuneCountInString(text) <= limit {
		return text
	}
	return string([]rune(text)[:limit]) + "…"
}
