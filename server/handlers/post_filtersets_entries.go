package serverhandlers

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/logkey"
)

// Sizes of gen_filtersets.fset_name and gen_filters.f_value.
const (
	filtersetNameMaxLen = 100
	filterValueMaxLen   = 256
)

// newFiltersetEntry is a validated entry of a filterset being created: a filter
// definition, or the id of a filterset to nest.
type newFiltersetEntry struct {
	logOp  string
	table  string
	field  string
	op     string
	value  string
	encap  int
	nested bool
}

// checkNewFiltersetEntries validates the entries of a filterset being created and
// resolves its nested filtersets. The error returned is meant for the client.
func checkNewFiltersetEntries(ctx context.Context, odb *cdb.DB, entries []server.FiltersetNewEntry) ([]newFiltersetEntry, int, error) {
	out := make([]newFiltersetEntry, 0, len(entries))
	for i, entry := range entries {
		n := i + 1
		e := newFiltersetEntry{logOp: string(entry.FLogOp)}
		if !slices.Contains(filtersetLogOps, e.logOp) {
			return nil, http.StatusBadRequest, fmt.Errorf("entry %d: f_log_op must be one of %s", n, strings.Join(filtersetLogOps, ", "))
		}
		isFilter := entry.FTable != nil || entry.FField != nil || entry.FOp != nil || entry.FValue != nil
		isNested := entry.Filterset != nil && *entry.Filterset != ""
		switch {
		case isFilter && isNested:
			return nil, http.StatusBadRequest, fmt.Errorf("entry %d: a filter or a filterset, not both", n)
		case isNested:
			id, _, err := odb.FiltersetByIDOrName(ctx, *entry.Filterset)
			if err != nil {
				return nil, http.StatusInternalServerError, fmt.Errorf("entry %d: cannot lookup filterset", n)
			}
			if id == 0 {
				return nil, http.StatusNotFound, fmt.Errorf("entry %d: filterset %s does not exist", n, *entry.Filterset)
			}
			e.encap, e.nested = id, true
		case isFilter:
			if entry.FTable == nil || entry.FField == nil || entry.FOp == nil || entry.FValue == nil ||
				*entry.FTable == "" || *entry.FField == "" || *entry.FValue == "" {
				return nil, http.StatusBadRequest, fmt.Errorf("entry %d: f_table, f_field, f_op and f_value are mandatory", n)
			}
			e.table, e.field, e.op, e.value = *entry.FTable, *entry.FField, strings.ToUpper(string(*entry.FOp)), *entry.FValue
			if utf8.RuneCountInString(e.value) > filterValueMaxLen {
				return nil, http.StatusBadRequest, fmt.Errorf("entry %d: f_value is longer than %d characters", n, filterValueMaxLen)
			}
			if !slices.Contains(filterTables, e.table) {
				return nil, http.StatusBadRequest, fmt.Errorf("entry %d: f_table must be one of %s", n, strings.Join(filterTables, ", "))
			}
			if !slices.Contains(filterOperators, e.op) {
				return nil, http.StatusBadRequest, fmt.Errorf("entry %d: f_op must be one of %s", n, strings.Join(filterOperators, ", "))
			}
			found, err := odb.ColumnExists(ctx, e.table, e.field)
			if err != nil {
				return nil, http.StatusInternalServerError, fmt.Errorf("entry %d: cannot check filter field", n)
			}
			if !found {
				return nil, http.StatusBadRequest, fmt.Errorf("entry %d: field %s not found in table %s", n, e.field, e.table)
			}
		default:
			return nil, http.StatusBadRequest, fmt.Errorf("entry %d: a filter or a filterset is mandatory", n)
		}
		out = append(out, e)
	}
	return out, 0, nil
}

// postFiltersetWithEntries creates a filterset with its entries in one transaction,
// reusing the filters already defined and creating the others.
func (a *Api) postFiltersetWithEntries(c echo.Context, log *slog.Logger, ctx context.Context, name, stats string, entries []server.FiltersetNewEntry) error {
	odb := a.ODB

	if utf8.RuneCountInString(name) > filtersetNameMaxLen {
		return JSONProblemf(c, http.StatusBadRequest, "fset_name is longer than %d characters", filtersetNameMaxLen)
	}

	_, exists, err := odb.FiltersetByName(ctx, name)
	if err != nil {
		log.Error("cannot check filterset existence", "fset_name", name, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check filterset existence")
	}
	if exists {
		return JSONProblemf(c, http.StatusConflict, "a filterset named %s already exists", name)
	}

	checked, status, err := checkNewFiltersetEntries(ctx, odb, entries)
	if err != nil {
		return JSONProblem(c, status, err.Error())
	}

	userEmail, _ := c.Get(XUserEmail).(string)

	tx, markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		log.Error("cannot start transaction", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot create filterset")
	}
	// The transaction ends before the reply, which reads the filterset back outside it.
	ended := false
	defer func() {
		if !ended {
			endTx()
		}
	}()

	id, err := tx.InsertFilterset(ctx, name, stats, userEmail)
	if err != nil {
		log.Error("cannot create filterset", "fset_name", name, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot create filterset")
	}
	logs := []cdb.LogEntry{{
		Action: "filterset.create",
		User:   userEmail,
		Fmt:    "added filterset %(name)s",
		Dict:   map[string]any{"name": name},
		Level:  "info",
	}}

	for i, e := range checked {
		order := i + 1
		if e.nested {
			if err := tx.InsertFiltersetEncap(ctx, id, e.encap, order, e.logOp); err != nil {
				log.Error("cannot nest filterset", "fset_id", id, "encap_fset_id", e.encap, logkey.Error, err)
				return JSONProblemf(c, http.StatusInternalServerError, "cannot create filterset")
			}
			continue
		}
		fID, found, err := tx.FilterByDefinition(ctx, e.table, e.field, e.op, e.value)
		if err != nil {
			log.Error("cannot check filter existence", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot create filterset")
		}
		if !found {
			fID, err = tx.InsertFilter(ctx, e.table, e.field, e.op, e.value, userEmail)
			if err != nil {
				log.Error("cannot create filter", logkey.Error, err)
				return JSONProblemf(c, http.StatusInternalServerError, "cannot create filterset")
			}
			logs = append(logs, cdb.LogEntry{
				Action: "filter.create",
				User:   userEmail,
				Fmt:    "added filter %(t)s.%(f)s %(o)s %(val)s",
				Dict:   map[string]any{"t": e.table, "f": e.field, "o": e.op, "val": e.value},
				Level:  "info",
			})
		}
		if err := tx.InsertFiltersetFilter(ctx, id, fID, order, e.logOp); err != nil {
			log.Error("cannot attach filter", "fset_id", id, "f_id", fID, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot create filterset")
		}
	}

	for _, entry := range logs {
		if logErr := tx.Log(ctx, entry); logErr != nil {
			log.Error("cannot write audit log", logkey.Error, logErr)
		}
	}

	markSuccess()
	endTx()
	ended = true

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return a.handleItem(c, "PostFiltersets", "filterset", "id", strconv.Itoa(id), listEndpointParams{},
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return odb.GetFilterset(ctx, strconv.Itoa(id), p)
		})
}
