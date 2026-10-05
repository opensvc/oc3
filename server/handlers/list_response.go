package serverhandlers

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/opensvc/oc3/server"
)

type listMeta struct {
	Count          int            `json:"count,omitempty"`
	AvailableProps []string       `json:"available_props,omitempty"`
	Distinct       map[string]int `json:"distinct,omitempty"`
	Other          map[string]int `json:"other,omitempty"`
	IncludedProps  []string       `json:"included_props,omitempty"`
	Limit          int            `json:"limit,omitempty"`
	Offset         int            `json:"offset,omitempty"`
	Total          *int           `json:"total,omitempty"`
}

type listResponse struct {
	Data any       `json:"data"`
	Meta *listMeta `json:"meta,omitempty"`
}

func newDataResponse(items []map[string]any) listResponse {
	return listResponse{Data: items}
}

func queryWithMeta(meta *server.InQueryMeta) bool {
	if meta == nil {
		return true
	}

	switch strings.ToLower(strings.TrimSpace(string(*meta))) {
	case "0", "false":
		return false
	default:
		return true
	}
}

func queryWithStats(stats *server.InQueryStats) bool {
	if stats == nil {
		return false
	}

	switch strings.ToLower(strings.TrimSpace(string(*stats))) {
	case "1", "true":
		return true
	default:
		return false
	}
}

func statsValueKey(value any) string {
	if value == nil {
		return "empty"
	}

	switch v := value.(type) {
	case map[string]any, []any:
		b, err := json.Marshal(v)
		if err == nil {
			return string(b)
		}
	case string:
		if strings.TrimSpace(v) == "" {
			return "empty"
		}
		return v
	}

	s := strings.TrimSpace(fmt.Sprint(value))
	if s == "" {
		return "empty"
	}

	return s
}

func buildStatsData(items []map[string]any, props []string) (map[string]map[string]int, map[string]int) {
	statsData := make(map[string]map[string]int, len(props))
	distinct := make(map[string]int, len(props))

	for _, prop := range props {
		values := make(map[string]int)
		for _, item := range items {
			values[statsValueKey(item[prop])]++
		}
		statsData[prop] = values
		distinct[prop] = len(values)
	}

	return statsData, distinct
}

// newStatsResponse counts the values of props over items, the whole selection of
// a list whose pipeline does not go through handleList; limit > 0 keeps the most
// frequent values of each prop. See listStats.
func newStatsResponse(items []map[string]any, props []string, limit int) listResponse {
	values, distinct := buildStatsData(items, props)
	other := make(map[string]int, len(props))
	for _, prop := range props {
		values[prop] = topValues(values[prop], limit)
		other[prop] = len(items) - sumCounts(values[prop])
	}
	return listResponse{
		Data: values,
		Meta: &listMeta{Distinct: distinct, Other: other, Total: intPtr(len(items))},
	}
}

func newListResponse(items []map[string]any, mapping propMapping, query ListQueryParameters) listResponse {
	response := listResponse{
		Data: items,
	}

	if !query.WithMeta {
		return response
	}

	response.Meta = &listMeta{
		Count:          len(items),
		AvailableProps: availableProps(mapping),
		IncludedProps:  query.Props,
		Limit:          query.Page.Limit,
		Offset:         query.Page.Offset,
	}

	return response
}

func intPtr(n int) *int { return &n }

// withTotal sets meta.total, the number of rows of the list without pagination,
// as the historical collector returned it along with every paginated answer. A
// response without meta is left alone: meta=false asks for the rows only.
func (r listResponse) withTotal(total *int) listResponse {
	if r.Meta != nil && total != nil {
		r.Meta.Total = total
	}
	return r
}
