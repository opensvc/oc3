package serverhandlers

import (
	"strings"

	"github.com/opensvc/oc3/server"
)

type listMeta struct {
	Count          int      `json:"count"`
	AvailableProps []string `json:"available_props"`
	IncludedProps  []string `json:"included_props"`
	Limit          int      `json:"limit"`
	Offset         int      `json:"offset"`
}

type listResponse struct {
	Data []map[string]any `json:"data"`
	Meta *listMeta        `json:"meta,omitempty"`
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

func newListResponse(items []map[string]any, mapping propMapping, includedProps []string, page PageParams, withMeta bool) listResponse {
	response := listResponse{
		Data: items,
	}

	if !withMeta {
		return response
	}

	response.Meta = &listMeta{
		Count:          len(items),
		AvailableProps: allowedProps(mapping),
		IncludedProps:  includedProps,
		Limit:          page.Limit,
		Offset:         page.Offset,
	}

	return response
}
