package serverhandlers

import "github.com/opensvc/oc3/server"

type ListQueryParameters struct {
	Page      PageParams
	Props     []string
	WithMeta  bool
	WithStats bool
}

func buildListQueryParameters(
	props *server.InQueryProps,
	limit *server.InQueryLimit,
	offset *server.InQueryOffset,
	meta *server.InQueryMeta,
	stats *server.InQueryStats,
	mapping propMapping,
) (ListQueryParameters, error) {
	selectedProps, err := buildProps(props, mapping)
	if err != nil {
		return ListQueryParameters{}, err
	}

	return ListQueryParameters{
		Page:      buildPageParams(limit, offset),
		Props:     selectedProps,
		WithMeta:  queryWithMeta(meta),
		WithStats: queryWithStats(stats),
	}, nil
}
