package serverhandlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"gopkg.in/yaml.v3"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
)

// GetForms handles GET /forms: the forms published to the caller, all of them
// for a manager.
func (a *Api) GetForms(c echo.Context, params server.GetFormsParams) error {
	return a.handleList(c, "GetForms", "form", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter, virtual: formDefinitionProp,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetForms(ctx, nil, p)
	})
}

// GetForm handles GET /forms/{form_id}: as GET /forms for one id, empty rather
// than 404 when the form is not visible, as the historical line handler does.
func (a *Api) GetForm(c echo.Context, formId int, params server.GetFormParams) error {
	id := int64(formId)
	return a.handleList(c, "GetForm", "form", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter, virtual: formDefinitionProp,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetForms(ctx, &id, p)
	})
}

// formResponse reads a form back with its default props, as the historical
// handlers answer a change with rest_get_form().handler(id). The caller has just
// written it: it is read without the publication filter.
func (a *Api) formResponse(ctx context.Context, formID int64) (map[string]any, error) {
	items, err := a.defaultRows(ctx, "form", formDefinitionProp, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetForms(ctx, &formID, p)
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"data": items}, nil
}

// formFields turns posted keys into forms columns: form_definition, a JSON
// document or its string form, becomes form_yaml, and a posted form_yaml must
// parse. Keys that are not columns are refused, where the historical collector
// failed on the database insert.
func formFields(entry map[string]any) (map[string]any, error) {
	fields := map[string]any{}
	keys := make([]string, 0, len(entry))
	for k := range entry {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var invalid []string
	for _, k := range keys {
		v := entry[k]
		switch {
		case k == "id":
			// The path names the form; the historical handler drops the key too.
		case k == "form_definition":
			definition := v
			if s, ok := v.(string); ok {
				if err := json.Unmarshal([]byte(s), &definition); err != nil {
					return nil, httpErrorf(http.StatusBadRequest, "invalid form_definition: %s", err)
				}
			}
			b, err := yaml.Marshal(numbersAsValues(definition))
			if err != nil {
				return nil, httpErrorf(http.StatusBadRequest, "invalid form_definition: %s", err)
			}
			fields["form_yaml"] = string(b)
		case cdb.IsFormColumn(k):
			s, _ := entryString(entry, k)
			if v == nil {
				fields[k] = nil
				continue
			}
			if k == "form_yaml" {
				if _, err := parseFormYaml(s); err != nil {
					return nil, httpErrorf(http.StatusBadRequest, "invalid form_yaml: %s", err)
				}
			}
			fields[k] = s
		default:
			invalid = append(invalid, k)
		}
	}
	if len(invalid) > 0 {
		return nil, httpErrorf(http.StatusBadRequest, "invalid properties: %s", strings.Join(invalid, ", "))
	}
	return fields, nil
}

// numbersAsValues converts the json.Number of a decoded body to plain numbers,
// so that the yaml encoder writes them as numbers and not as strings.
func numbersAsValues(v any) any {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		if f, err := t.Float64(); err == nil {
			return f
		}
		return t.String()
	case map[string]any:
		for k, item := range t {
			t[k] = numbersAsValues(item)
		}
	case []any:
		for i, item := range t {
			t[i] = numbersAsValues(item)
		}
	}
	return v
}

// PostForms handles POST /forms: create forms.
func (a *Api) PostForms(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostForms")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if err := requireFormsManager(c); err != nil {
		return httpProblem(c, err)
	}
	entries, isList, err := decodeEntries(c)
	if err != nil {
		return httpProblem(c, err)
	}
	caller, err := a.formCaller(ctx, c)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the caller", err))
	}
	defer a.formNotify(ctx, log)
	return runEntries(c, entries, isList, func(entry map[string]any) (map[string]any, error) {
		return a.createForm(ctx, c, log, caller, entry)
	})
}

func (a *Api) createForm(ctx context.Context, c echo.Context, log *slog.Logger, caller formCaller, entry map[string]any) (map[string]any, error) {
	name, ok := entryString(entry, "form_name")
	if !ok || name == "" {
		return nil, httpErrorf(http.StatusBadRequest, "Key 'form_name' is mandatory")
	}
	fields, err := formFields(entry)
	if err != nil {
		return nil, err
	}
	if _, exists, err := a.ODB.FormIDByName(ctx, name); err != nil {
		return nil, httpInternal(log, "cannot check the form name", err)
	} else if exists {
		return nil, httpErrorf(http.StatusConflict, "a form named %s already exists", name)
	}
	fields["form_created"] = time.Now().Format(time.DateTime)
	fields["form_author"] = caller.name

	id, err := a.ODB.InsertForm(ctx, fields)
	if err != nil {
		return nil, httpInternal(log, "cannot create the form", err)
	}

	// The creator's default group is made responsible for the form and the form
	// is published to it, as lib_forms_add_default_team_*() do.
	if caller.id != 0 {
		groupID, found, err := a.ODB.UserDefaultGroupID(ctx, caller.id)
		if err != nil {
			return nil, httpInternal(log, "cannot read the default group", err)
		}
		if found {
			for _, table := range []cdb.FormTeamTable{cdb.FormResponsibles, cdb.FormPublications} {
				if err := a.ODB.InsertFormTeam(ctx, table, id, groupID); err != nil {
					return nil, httpInternal(log, "cannot link the form to the default group", err)
				}
			}
		}
	}
	if yamlText, ok := fields["form_yaml"].(string); ok {
		a.formCommit(log, id, yamlText, caller)
	}

	a.formLog(ctx, c, log, "form.add", "Form %(form_name)s added", map[string]any{"form_name": name})
	return a.formResponse(ctx, id)
}

// PostForm handles POST /forms/{form_id}: modify a form.
func (a *Api) PostForm(c echo.Context, formId int) error {
	log := echolog.GetLogHandler(c, "PostForm")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if err := requireFormsManager(c); err != nil {
		return httpProblem(c, err)
	}
	entries, isList, err := decodeEntries(c)
	if err != nil {
		return httpProblem(c, err)
	}
	if isList {
		return JSONProblem(c, http.StatusBadRequest, "expecting an object")
	}
	result, err := a.updateForm(ctx, c, log, int64(formId), entries[0])
	if err != nil {
		return httpProblem(c, err)
	}
	a.formNotify(ctx, log)
	return c.JSON(http.StatusOK, result)
}

func (a *Api) updateForm(ctx context.Context, c echo.Context, log *slog.Logger, id int64, entry map[string]any) (map[string]any, error) {
	form, err := a.ODB.FormByID(ctx, id)
	if err != nil {
		return nil, httpInternal(log, "cannot read the form", err)
	}
	if form == nil {
		return nil, httpErrorf(http.StatusNotFound, "Form %d not found", id)
	}
	fields, err := formFields(entry)
	if err != nil {
		return nil, err
	}
	if name, ok := fields["form_name"].(string); ok && name != form.Name {
		if otherID, exists, err := a.ODB.FormIDByName(ctx, name); err != nil {
			return nil, httpInternal(log, "cannot check the form name", err)
		} else if exists && otherID != id {
			return nil, httpErrorf(http.StatusConflict, "a form named %s already exists", name)
		}
	}
	if err := a.ODB.UpdateForm(ctx, id, fields); err != nil {
		return nil, httpInternal(log, "cannot update the form", err)
	}

	d := map[string]any{"form_name": form.Name, "data": formChanges(form, fields)}
	a.formLog(ctx, c, log, "form.change", "Form %(form_name)s change: %(data)s", d)

	if yamlText, ok := fields["form_yaml"].(string); ok && yamlText != "" {
		caller, err := a.formCaller(ctx, c)
		if err != nil {
			return nil, httpInternal(log, "cannot read the caller", err)
		}
		a.formCommit(log, id, yamlText, caller)
	}

	result, err := a.formResponse(ctx, id)
	if err != nil {
		return nil, httpInternal(log, "cannot read the form back", err)
	}
	result["info"] = pyFormat("Form %(form_name)s change: %(data)s", d)
	return result, nil
}

// formChanges describes a change as beautify_change() does: "key: old => new",
// for the changed columns, sorted.
func formChanges(form *cdb.Form, fields map[string]any) string {
	current := map[string]string{
		"form_name": form.Name, "form_yaml": form.Yaml, "form_author": form.Author,
		"form_created": form.Created, "form_type": form.Type, "form_folder": form.Folder,
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []string
	for _, k := range keys {
		old, known := current[k]
		if !known {
			continue
		}
		value := ""
		if fields[k] != nil {
			value = fmt.Sprint(fields[k])
		}
		if old != value {
			out = append(out, fmt.Sprintf("%s: %s => %s", k, old, value))
		}
	}
	return strings.Join(out, ", ")
}

// DeleteForm handles DELETE /forms/{form_id}.
func (a *Api) DeleteForm(c echo.Context, formId int) error {
	log := echolog.GetLogHandler(c, "DeleteForm")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	result, err := a.deleteForm(ctx, c, log, int64(formId))
	if err != nil {
		return httpProblem(c, err)
	}
	a.formNotify(ctx, log)
	return c.JSON(http.StatusOK, result)
}

// DeleteForms handles DELETE /forms: the forms named by the 'id' key.
func (a *Api) DeleteForms(c echo.Context) error {
	log := echolog.GetLogHandler(c, "DeleteForms")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	entries, isList, err := decodeEntries(c)
	if err != nil {
		return httpProblem(c, err)
	}
	defer a.formNotify(ctx, log)
	return runEntries(c, entries, isList, func(entry map[string]any) (map[string]any, error) {
		s, ok := entryString(entry, "id")
		if !ok {
			return nil, httpErrorf(http.StatusBadRequest, "The 'id' key is mandatory")
		}
		id, err := parseFormID(s)
		if err != nil {
			return nil, err
		}
		return a.deleteForm(ctx, c, log, id)
	})
}

func (a *Api) deleteForm(ctx context.Context, c echo.Context, log *slog.Logger, id int64) (map[string]any, error) {
	if err := requireFormsManager(c); err != nil {
		return nil, err
	}
	if err := a.requireFormResponsible(c, log, ctx, id); err != nil {
		return nil, err
	}
	form, err := a.ODB.FormByID(ctx, id)
	if err != nil {
		return nil, httpInternal(log, "cannot read the form", err)
	}
	if form == nil {
		return nil, httpErrorf(http.StatusNotFound, "Form %d not found", id)
	}
	if err := a.ODB.DeleteForm(ctx, id); err != nil {
		return nil, httpInternal(log, "cannot delete the form", err)
	}
	d := map[string]any{"form_name": form.Name}
	a.formLog(ctx, c, log, "form.del", "Form %(form_name)s deleted", d)
	return map[string]any{"info": pyFormat("Form %(form_name)s deleted", d)}, nil
}

// GetFormAmIResponsible handles GET /forms/{form_id}/am_i_responsible.
func (a *Api) GetFormAmIResponsible(c echo.Context, formId int) error {
	log := echolog.GetLogHandler(c, "GetFormAmIResponsible")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	ok, err := a.ODB.FormResponsible(ctx, int64(formId), UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot check form responsibility", err))
	}
	return c.JSON(http.StatusOK, map[string]bool{"data": ok})
}

// formIDString renders a form id as the historical messages print it.
func formIDString(id int64) string { return strconv.FormatInt(id, 10) }
