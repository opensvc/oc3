package serverhandlers

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

const (
	// searchMinLength is the shortest text searched: one character matches
	// almost every row and tells nothing.
	searchMinLength = 2
	// searchDefaultLimit and searchMaxLimit bound the hits returned per kind.
	searchDefaultLimit = 5
	searchMaxLimit     = 20
	// searchTimeout bounds the whole search, all kinds together.
	searchTimeout = 5 * time.Second
)

// searchKind says how the global search looks for one kind of object: the
// list of the collector it reads, with the access control of that list, the
// props whose value may contain the text and the props returned for each hit.
//
// match names the props that identify the object: its name, its ids. context
// names the other props shown beside the name in a result — the application of a
// node, the node of an instance — which may contain the text too. An object
// matching by what identifies it comes before one matching only by its context:
// "prd" finds the node named prd-db01 before the hundred nodes of the PRD
// environment.
type searchKind struct {
	kind    string
	mapping string
	match   []string
	context []string
	// idProp is also compared for equality when the text is an integer: a
	// request is named by its number.
	idProp  string
	props   []string
	orderby string
	// extra conditions, ANDed with the match.
	extra []cdb.ColumnFilter
	// withUserID forwards the caller's id, for the lists whose access control
	// references it.
	withUserID bool
	fetch      func(a *Api, ctx context.Context, p cdb.ListParams) ([]map[string]any, error)
	// search replaces the list query for a kind without one, the tags.
	search func(a *Api, ctx context.Context, pattern string, limit int) ([]map[string]any, error)
}

// searchKinds are the searched kinds, in the order the results are returned.
var searchKinds = []searchKind{
	{
		kind: "node", mapping: "node",
		match:   []string{"nodename", "node_id", "fqdn"},
		context: []string{"app", "node_env", "os_name"},
		props:   []string{"node_id", "nodename", "app", "node_env", "cluster_id", "fqdn", "os_name"},
		orderby: "nodename",
		fetch: func(a *Api, ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetNodes(ctx, p)
		},
	},
	{
		kind: "service", mapping: "service",
		match:   []string{"svcname", "svc_id"},
		context: []string{"svc_app", "svc_env", "svc_topology", "cluster_id"},
		props:   []string{"svc_id", "svcname", "svc_app", "svc_env", "cluster_id", "svc_availstatus", "svc_topology"},
		orderby: "svcname",
		fetch: func(a *Api, ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetServices(ctx, p)
		},
	},
	{
		kind: "instance", mapping: "instance",
		match:   []string{"services.svcname", "mon_vmname"},
		context: []string{"nodes.nodename", "svc_id"},
		props:   []string{"svc_id", "node_id", "mon_vmname", "mon_availstatus", "services.svcname", "nodes.nodename"},
		orderby: "services.svcname,nodes.nodename",
		fetch: func(a *Api, ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetServicesInstances(ctx, p)
		},
	},
	{
		kind: "app", mapping: "app",
		match:   []string{"app", "description"},
		context: []string{"app_domain"},
		props:   []string{"id", "app", "app_domain", "description"},
		orderby: "app",
		fetch: func(a *Api, ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetApps(ctx, p)
		},
	},
	{
		kind: "network", mapping: "node_ip",
		match:   []string{"addr", "mac"},
		context: []string{"nodename", "intf", "net_name"},
		props:   []string{"id", "addr", "mask", "mac", "intf", "node_id", "nodename", "net_name"},
		orderby: "addr",
		fetch: func(a *Api, ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetIps(ctx, p)
		},
	},
	{
		kind: "disk", mapping: "disk",
		match:   []string{"disk_id", "disk_name", "disk_devid"},
		context: []string{"nodename", "svcname", "disk_arrayid"},
		props:   []string{"disk_id", "disk_name", "disk_size", "disk_arrayid", "nodename", "svcname"},
		orderby: "disk_id",
		fetch: func(a *Api, ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetDisks(ctx, p)
		},
	},
	{
		kind: "tag",
		search: func(a *Api, ctx context.Context, pattern string, limit int) ([]map[string]any, error) {
			return a.ODB.SearchTags(ctx, pattern, limit)
		},
	},
	{
		kind: "user", mapping: "user",
		match:      []string{"email", "first_name", "last_name", "username"},
		props:      []string{"id", "email", "first_name", "last_name", "username"},
		orderby:    "email",
		withUserID: true,
		fetch: func(a *Api, ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetUsers(ctx, p)
		},
	},
	{
		kind: "group", mapping: "auth_group",
		match:   []string{"role", "description"},
		props:   []string{"id", "role", "privilege", "description"},
		orderby: "role",
		// The private group of each user would drown the teams.
		extra: []cdb.ColumnFilter{{Expr: `auth_group.role NOT LIKE 'user\_%'`}},
		fetch: func(a *Api, ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetGroups(ctx, p)
		},
	},
	{
		kind: "request", mapping: "workflow",
		match:   []string{"form_name", "last_form_name"},
		context: []string{"status", "creator"},
		idProp:  "id",
		props:   []string{"id", "form_name", "last_form_name", "status", "creator", "last_update"},
		orderby: "-id",
		fetch: func(a *Api, ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetWorkflows(ctx, nil, "", p)
		},
	},
	{
		kind: "moduleset", mapping: "moduleset",
		match:   []string{"modset_name"},
		context: []string{"modset_author"},
		props:   []string{"id", "modset_name", "modset_author"},
		orderby: "modset_name",
		fetch: func(a *Api, ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetComplianceModulesets(ctx, nil, p)
		},
	},
	{
		kind: "ruleset", mapping: "ruleset",
		match:   []string{"ruleset_name"},
		context: []string{"ruleset_type"},
		props:   []string{"id", "ruleset_name", "ruleset_type", "ruleset_public"},
		orderby: "ruleset_name",
		fetch: func(a *Api, ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetComplianceRulesets(ctx, nil, p)
		},
	},
	{
		kind: "filterset", mapping: "filterset",
		match:   []string{"fset_name"},
		context: []string{"fset_author"},
		props:   []string{"id", "fset_name", "fset_author"},
		orderby: "fset_name",
		fetch: func(a *Api, ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetFiltersets(ctx, p)
		},
	},
	{
		kind: "form", mapping: "form",
		match:   []string{"form_name", "form_folder"},
		context: []string{"form_type"},
		props:   []string{"id", "form_name", "form_type", "form_folder"},
		orderby: "form_name",
		fetch: func(a *Api, ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetForms(ctx, nil, p)
		},
	},
}

// searchGroup is the result of the search for one kind. More tells that other
// objects of the kind match beyond the ones returned.
type searchGroup struct {
	Kind  string           `json:"kind"`
	Items []map[string]any `json:"items"`
	More  bool             `json:"more"`
	Error string           `json:"error,omitempty"`
}

// GetSearch handles GET /search: the objects of the main kinds whose name, or
// another identifying prop, contains the text, as the historical collector's
// search did, then those whose context shown beside the name contains it. Each kind is read through its own list, with its access control,
// all kinds at once; a kind that fails is reported in its group without
// failing the others.
func (a *Api) GetSearch(c echo.Context, params server.GetSearchParams) error {
	log := echolog.GetLogHandler(c, "GetSearch")

	text := strings.TrimSpace(params.Q)
	limit := searchDefaultLimit
	if params.Limit != nil {
		limit = min(max(*params.Limit, 1), searchMaxLimit)
	}
	var wanted []string
	if params.Kinds != nil && *params.Kinds != "" {
		wanted = strings.Split(*params.Kinds, ",")
		for _, k := range wanted {
			if !slices.ContainsFunc(searchKinds, func(s searchKind) bool { return s.kind == k }) {
				return JSONProblemf(c, http.StatusBadRequest, "unknown kind %q", k)
			}
		}
	}

	groups := []searchGroup{}
	if utf8.RuneCountInString(text) < searchMinLength {
		return c.JSON(http.StatusOK, map[string]any{"data": groups})
	}

	ctx, cancel := context.WithTimeout(c.Request().Context(), searchTimeout)
	defer cancel()

	userID := authUserID(c)
	// The user id restricts the requests to those the caller takes part in.
	base := cdb.ListParams{Groups: UserGroupsFromContext(c), IsManager: IsManager(c), UserID: userID}
	pattern := "%" + likeEscaper.Replace(text) + "%"

	kinds := make([]searchKind, 0, len(searchKinds))
	for _, k := range searchKinds {
		if wanted == nil || slices.Contains(wanted, k.kind) {
			kinds = append(kinds, k)
		}
	}
	groups = make([]searchGroup, len(kinds))
	var wg sync.WaitGroup
	for i, k := range kinds {
		wg.Add(1)
		go func() {
			defer wg.Done()
			items, err := a.searchOne(ctx, k, text, pattern, limit, base, userID)
			groups[i] = searchGroup{Kind: k.kind, Items: items}
			if err != nil {
				log.Error("cannot search", "kind", k.kind, logkey.Error, err)
				groups[i].Error = fmt.Sprintf("cannot search %s", k.kind)
				groups[i].Items = []map[string]any{}
				return
			}
			if len(items) > limit {
				groups[i].Items = items[:limit]
				groups[i].More = true
			}
			if groups[i].Items == nil {
				groups[i].Items = []map[string]any{}
			}
		}()
	}
	wg.Wait()
	log.Info("called", "text", text, "kinds", len(kinds), "limit", limit)
	return c.JSON(http.StatusOK, map[string]any{"data": groups})
}

// searchOne runs the search of one kind, asking for one row more than limit
// to tell whether more match.
func (a *Api) searchOne(ctx context.Context, k searchKind, text, pattern string, limit int, base cdb.ListParams, userID *int64) ([]map[string]any, error) {
	if k.search != nil {
		return k.search(a, ctx, pattern, limit+1)
	}
	mapping := propsMapping[k.mapping]
	props := strings.Join(k.props, ",")
	orderby := k.orderby
	fetchLimit := limit + 1
	query, err := buildListQueryParameters(&props, &fetchLimit, nil, nil, nil, &orderby, nil, mapping)
	if err != nil {
		return nil, err
	}
	selectExprs, err := buildSelectClause(query.Props, mapping)
	if err != nil {
		return nil, err
	}

	named, err := searchCondition(k.match, mapping, pattern)
	if err != nil {
		return nil, err
	}
	if k.idProp != "" {
		if id, err := strconv.ParseInt(text, 10, 64); err == nil {
			col, err := resolvePropCol(k.idProp, mapping, "search")
			if err != nil {
				return nil, err
			}
			named.Expr = "(" + named.Expr + " OR " + col.Qualified() + " = ?)"
			named.Args = append(named.Args, id)
		}
	}

	p := base
	p.Limit = query.Page.Limit
	p.Props = query.Props
	p.SelectExprs = selectExprs
	p.TypeHints = buildTypeHints(query.Props, mapping)
	p.OrderBy = query.OrderBy
	p.Filters = append([]cdb.ColumnFilter{named}, k.extra...)
	if k.withUserID {
		p.UserID = userID
	}
	items, err := k.fetch(a, ctx, p)
	if err != nil || len(k.context) == 0 || len(items) >= fetchLimit {
		return items, err
	}

	// Room left: the objects matching by their context only, after the others.
	around, err := searchCondition(k.context, mapping, pattern)
	if err != nil {
		return nil, err
	}
	// A prop without a value makes the first condition neither true nor false:
	// such an object did not match by name, and is not to be left out here.
	around.Expr = "(" + around.Expr + " AND NOT COALESCE(" + named.Expr + ", FALSE))"
	around.Args = append(around.Args, named.Args...)
	p.Limit = fetchLimit - len(items)
	p.Filters = append([]cdb.ColumnFilter{around}, k.extra...)
	more, err := k.fetch(a, ctx, p)
	if err != nil {
		return nil, err
	}
	return append(items, more...), nil
}

// searchCondition is the condition "one of these props contains the text", on the
// columns of the props.
func searchCondition(props []string, mapping propMapping, pattern string) (cdb.ColumnFilter, error) {
	var (
		conds []string
		args  []any
		f     cdb.ColumnFilter
	)
	for _, prop := range props {
		col, err := resolvePropCol(prop, mapping, "search")
		if err != nil {
			return f, err
		}
		if f.Col == nil {
			f.Col = col
		}
		conds = append(conds, col.Qualified()+" LIKE ?")
		args = append(args, pattern)
	}
	f.Expr = "(" + strings.Join(conds, " OR ") + ")"
	f.Args = args
	return f, nil
}
