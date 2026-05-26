package serverhandlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetNodeAlerts handles GET /nodes/{node_id}/alerts.
func (a *Api) GetNodeAlerts(c echo.Context, nodeId string, params server.GetNodeAlertsParams) error {
	mapping := propsMapping["alert"]

	query, err := buildListQueryParameters(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, mapping)
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	log := echolog.GetLogHandler(c, "GetNodeAlerts")

	node, err := a.resolveNode(c, log, nodeId)
	if err != nil {
		return err
	}

	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)

	log.Info("called",
		logkey.NodeID, node.NodeID,
		"limit", query.Page.Limit, "offset", query.Page.Offset,
		"props", query.Props,
		"is_manager", isManager,
	)

	hasAlert := slices.Contains(query.Props, "alert")
	fetchProps := query.Props
	if hasAlert {
		fetchProps = stripProp(fetchProps, "alert")
		fetchProps = ensureProps(fetchProps, "dash_fmt", "dash_dict")
	}

	selectExprs, err := buildSelectClause(fetchProps, mapping)
	if err != nil {
		log.Error("cannot build select clause", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot build select clause")
	}

	items, err := a.ODB.GetNodeAlerts(c.Request().Context(), node.NodeID, cdb.ListParams{
		Groups:      groups,
		IsManager:   isManager,
		Limit:       query.Page.Limit,
		Offset:      query.Page.Offset,
		Props:       fetchProps,
		SelectExprs: selectExprs,
		TypeHints:   buildTypeHints(fetchProps, mapping),
		OrderBy:     query.OrderBy,
		GroupBy:     query.GroupBy,
	})
	if err != nil {
		log.Error("cannot fetch alerts", logkey.NodeID, node.NodeID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get alerts for node %s", node.NodeID)
	}

	if hasAlert {
		mangleAlerts(items, query.Props)
	}

	return c.JSON(http.StatusOK, newListResponse(items, mapping, query))
}

// mangleAlerts parses dash_dict as JSON and computes the "alert" string
func mangleAlerts(items []map[string]any, requestedProps []string) {
	wantFmt := slices.Contains(requestedProps, "dash_fmt")
	wantDict := slices.Contains(requestedProps, "dash_dict")

	for _, item := range items {
		fmtStr, _ := item["dash_fmt"].(string)
		dictStr, _ := item["dash_dict"].(string)

		var dict map[string]any
		if dictStr != "" {
			if err := json.Unmarshal([]byte(dictStr), &dict); err == nil {
				if wantDict {
					item["dash_dict"] = dict
				}
				if s, ok := formatNamedTemplate(fmtStr, dict); ok {
					item["alert"] = s
				}
			}
		}

		if !wantFmt {
			delete(item, "dash_fmt")
		}
		if !wantDict {
			delete(item, "dash_dict")
		}
	}
}

var namedTemplateRe = regexp.MustCompile(`%(?:%|\(([^)]+)\)([sdfxX]))`)

// formatNamedTemplate substitutes `%(name)s`-style placeholders using dict
func formatNamedTemplate(format string, dict map[string]any) (string, bool) {
	if format == "" {
		return "", true
	}
	ok := true
	out := namedTemplateRe.ReplaceAllStringFunc(format, func(match string) string {
		if match == "%%" {
			return "%"
		}
		sub := namedTemplateRe.FindStringSubmatch(match)
		key := sub[1]
		verb := sub[2]
		val, present := dict[key]
		if !present {
			ok = false
			return match
		}
		switch verb {
		case "s":
			return fmt.Sprintf("%v", val)
		case "d":
			return fmt.Sprintf("%d", toInt64(val))
		case "f":
			return fmt.Sprintf("%f", toFloat64(val))
		case "x":
			return fmt.Sprintf("%x", toInt64(val))
		case "X":
			return fmt.Sprintf("%X", toInt64(val))
		}
		return match
	})
	if !ok {
		return "", false
	}
	return out, true
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int64:
		return x
	case float64:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	case json.Number:
		n, _ := x.Int64()
		return n
	}
	return 0
}

func toFloat64(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	case json.Number:
		f, _ := x.Float64()
		return f
	}
	return 0
}

func stripProp(s []string, v string) []string {
	out := make([]string, 0, len(s))
	for _, e := range s {
		if e == v {
			continue
		}
		out = append(out, e)
	}
	return out
}

func ensureProps(s []string, extras ...string) []string {
	out := s
	for _, e := range extras {
		if !slices.Contains(out, e) {
			out = append(out, e)
		}
	}
	return out
}
