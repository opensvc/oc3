package serverhandlers

import "github.com/opensvc/oc3/server"

const (
	defaultPageLimit = 50
)

type PageParams struct {
	Limit  int
	Offset int
}

func buildPageParams(limit *server.InQueryLimit, offset *server.InQueryOffset) PageParams {
	page := PageParams{
		Limit:  defaultPageLimit,
		Offset: 0,
	}

	if limit != nil {
		page.Limit = int(*limit)
	}
	if offset != nil {
		page.Offset = int(*offset)
	}

	if page.Limit < 0 {
		page.Limit = defaultPageLimit
	}
	if page.Offset < 0 {
		page.Offset = 0
	}

	return page
}
