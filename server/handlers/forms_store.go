package serverhandlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
)

// GetFormsRevisions handles GET /forms_revisions.
func (a *Api) GetFormsRevisions(c echo.Context, params server.GetFormsRevisionsParams) error {
	return a.handleList(c, "GetFormsRevisions", "form_revision", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter, virtual: formDefinitionProp,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetFormsRevisions(ctx, nil, p)
	})
}

// GetFormsRevision handles GET /forms_revisions/{revision_id}: by id or md5.
func (a *Api) GetFormsRevision(c echo.Context, revisionId string, params server.GetFormsRevisionParams) error {
	return a.handleList(c, "GetFormsRevision", "form_revision", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter, virtual: formDefinitionProp,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetFormsRevisions(ctx, &revisionId, p)
	})
}

// GetFormsStore handles GET /forms_store.
func (a *Api) GetFormsStore(c echo.Context, params server.GetFormsStoreParams) error {
	return a.handleList(c, "GetFormsStore", "form_store", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter, virtual: formDefinitionProp,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetFormsStore(ctx, nil, p)
	})
}

// GetFormStore handles GET /forms_store/{store_id}.
func (a *Api) GetFormStore(c echo.Context, storeId int, params server.GetFormStoreParams) error {
	id := int64(storeId)
	return a.handleList(c, "GetFormStore", "form_store", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter, virtual: formDefinitionProp,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetFormsStore(ctx, &id, p)
	})
}

// defaultRows reads rows with the default props of a mapping, virtual props
// computed, for the handlers answering with records they assemble.
func (a *Api) defaultRows(ctx context.Context, mappingKey string, virtual map[string]virtualProp, fetch listFetcher) ([]map[string]any, error) {
	mapping := propsMapping[mappingKey]
	query, err := buildListQueryParameters(nil, nil, nil, nil, nil, nil, nil, mapping)
	if err != nil {
		return nil, err
	}
	fetchProps := fetchPropsFor(query.Props, virtual)
	selectExprs, err := buildSelectClause(fetchProps, mapping)
	if err != nil {
		return nil, err
	}
	items, err := fetch(ctx, cdb.ListParams{
		IsManager: true, Props: fetchProps, SelectExprs: selectExprs,
		TypeHints: buildTypeHints(fetchProps, mapping),
	})
	if err != nil {
		return nil, err
	}
	computeVirtualProps(items, query.Props, virtual)
	return items, nil
}

func (a *Api) storedForm(ctx context.Context, id int64) (map[string]any, error) {
	rows, err := a.defaultRows(ctx, "form_store", formDefinitionProp, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetFormsStore(ctx, &id, p)
	})
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return rows[0], nil
}

// asInt64 reads an id column of a row.
func asInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case int:
		return int64(t), true
	case string:
		n, err := strconv.ParseInt(t, 10, 64)
		return n, err == nil
	case []byte:
		n, err := strconv.ParseInt(string(t), 10, 64)
		return n, err == nil
	}
	return 0, false
}

// workflowDump returns a workflow with its head and tail stored forms, as
// /workflows/{id}/dump does in the historical collector.
func (a *Api) workflowDump(ctx context.Context, workflowID int64) (map[string]any, error) {
	rows, err := a.defaultRows(ctx, "workflow", nil, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetWorkflows(ctx, &workflowID, p)
	})
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	wf := rows[0]
	if headID, ok := asInt64(wf["form_head_id"]); ok {
		if wf["head"], err = a.storedForm(ctx, headID); err != nil {
			return nil, err
		}
	}
	if lastID, ok := asInt64(wf["last_form_id"]); ok {
		if wf["tail"], err = a.storedForm(ctx, lastID); err != nil {
			return nil, err
		}
	}
	return wf, nil
}

// GetFormStoreDump handles GET /forms_store/{store_id}/dump: the stored form
// with its workflow, the workflow carrying its head and tail stored forms.
func (a *Api) GetFormStoreDump(c echo.Context, storeId int) error {
	log := echolog.GetLogHandler(c, "GetFormStoreDump")
	ctx := c.Request().Context()
	stored, err := a.storedForm(ctx, int64(storeId))
	if err != nil {
		return formProblem(c, formInternal(log, "cannot read the stored form", err))
	}
	if stored == nil {
		return JSONProblemf(c, http.StatusNotFound, "stored form %d not found", storeId)
	}
	if headID, ok := asInt64(stored["form_head_id"]); ok {
		workflowID, found, err := a.ODB.WorkflowIDByHead(ctx, headID)
		if err != nil {
			return formProblem(c, formInternal(log, "cannot read the workflow", err))
		}
		if found {
			if stored["workflow"], err = a.workflowDump(ctx, workflowID); err != nil {
				return formProblem(c, formInternal(log, "cannot read the workflow", err))
			}
		}
	}
	return c.JSON(http.StatusOK, map[string]any{"data": []any{stored}})
}

// formResultsAccess describes the caller for the results access filter.
func formResultsAccess(c echo.Context) cdb.FormResultsAccess {
	nodeID, _ := c.Get(XNodeID).(string)
	return cdb.FormResultsAccess{IsManager: IsManager(c), UserID: authUserID(c), NodeID: nodeID}
}

// GetFormOutputResults handles GET /form_output_results/{results_id}: the
// results structure as stored.
func (a *Api) GetFormOutputResults(c echo.Context, resultsId int) error {
	log := echolog.GetLogHandler(c, "GetFormOutputResults")
	results, err := a.readFormResults(c.Request().Context(), c, int64(resultsId))
	if err != nil {
		return formProblem(c, formResultsError(log, err))
	}
	return c.JSON(http.StatusOK, results)
}

// formResultsError keeps a refusal as it is and logs an internal failure.
func formResultsError(log *slog.Logger, err error) error {
	var fe *formError
	if errors.As(err, &fe) {
		return err
	}
	return formInternal(log, "cannot read the results", err)
}

// readFormResults reads a results structure the caller may see, 404 otherwise.
func (a *Api) readFormResults(ctx context.Context, c echo.Context, id int64) (map[string]any, error) {
	s, found, err := a.ODB.FormOutputResults(ctx, id, formResultsAccess(c))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, formErrorf(http.StatusNotFound, "results not found")
	}
	var results map[string]any
	decoder := json.NewDecoder(bytes.NewReader([]byte(s)))
	decoder.UseNumber()
	if err := decoder.Decode(&results); err != nil {
		return nil, formErrorf(http.StatusInternalServerError, "unreadable results: %s", err)
	}
	return results, nil
}

// PutFormOutputResults handles PUT /form_output_results/{results_id}: add a
// result and log lines to an output, as scripts run by a form report them.
func (a *Api) PutFormOutputResults(c echo.Context, resultsId int) error {
	log := echolog.GetLogHandler(c, "PutFormOutputResults")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	id := int64(resultsId)
	results, err := a.readFormResults(ctx, c, id)
	if err != nil {
		return formProblem(c, formResultsError(log, err))
	}
	entries, isList, err := decodeEntries(c)
	if err != nil {
		return formProblem(c, err)
	}
	if isList {
		return JSONProblem(c, http.StatusBadRequest, "expecting an object")
	}
	body := entries[0]
	outputID, _ := entryString(body, "output_id")
	result, hasResult := body["result"]
	logLines, hasLog := body["log"]
	if (!hasResult || isEmpty(result)) && (!hasLog || isEmpty(logLines)) {
		return c.JSON(http.StatusOK, results)
	}
	if hasResult && !isEmpty(result) {
		outputs := ensureMap(results, "outputs")
		list, _ := outputs[outputID].([]any)
		outputs[outputID] = append(list, decodedJSON(result))
	}
	if hasLog && !isEmpty(logLines) {
		logs := ensureMap(results, "log")
		list, _ := logs[outputID].([]any)
		switch lines := decodedJSON(logLines).(type) {
		case []any:
			list = append(list, lines...)
		default:
			list = append(list, lines)
		}
		logs[outputID] = list
	}
	b, err := json.Marshal(results)
	if err != nil {
		return formProblem(c, formInternal(log, "cannot encode the results", err))
	}
	if err := a.ODB.UpdateFormOutputResults(ctx, id, string(b)); err != nil {
		return formProblem(c, formInternal(log, "cannot store the results", err))
	}
	a.formNotify(ctx, log)
	return c.JSON(http.StatusOK, results)
}

func isEmpty(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case string:
		return t == ""
	case []any:
		return len(t) == 0
	case map[string]any:
		return len(t) == 0
	}
	return false
}

// decodedJSON decodes a value posted as its JSON string form, as
// sjson.loads(result) does, and keeps any other value as it is.
func decodedJSON(v any) any {
	s, ok := v.(string)
	if !ok {
		return v
	}
	var out any
	decoder := json.NewDecoder(bytes.NewReader([]byte(s)))
	decoder.UseNumber()
	if err := decoder.Decode(&out); err != nil {
		return s
	}
	return out
}

func ensureMap(m map[string]any, key string) map[string]any {
	sub, ok := m[key].(map[string]any)
	if !ok {
		sub = map[string]any{}
		m[key] = sub
	}
	return sub
}
