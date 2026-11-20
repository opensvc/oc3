package cdb

import (
	"regexp"
	"slices"
	"strings"
)

// joinedMask is a table joined to the rows of a list for its "table.column"
// props: its rows of an app the caller is not responsible for show their
// identity only, the other columns read NULL.
type joinedMask struct {
	// table is the name the columns are selected with: "nodes", "services".
	table string
	// appCol is the column of its app: "nodes.app", "services.svc_app".
	appCol string
	// keep are the columns shown whatever the app: the identity of the row.
	keep []string
}

var (
	// nodesMask shows a node of another app by its id and its name: where an
	// instance of a service of the caller runs, for example.
	nodesMask = joinedMask{table: "nodes", appCol: "nodes.app", keep: []string{"node_id", "nodename"}}
	// servicesMask shows a service of another app by its id and its name.
	servicesMask = joinedMask{table: "services", appCol: "services.svc_app", keep: []string{"svc_id", "svcname"}}
)

// joinedColumnRe is a select expression naming a column of a joined table, as
// the "table.column" props are selected.
var joinedColumnRe = regexp.MustCompile(`^([a-z_]+)\.([a-z0-9_]+)$`)

// maskJoinedColumns returns the select expressions with the columns of the
// masked tables shown only for the rows of an app the caller's groups are
// responsible for, their identity aside, and the arguments of those
// expressions, to put before the other arguments of the query: the select
// list comes first. A row visible to the caller can join a node or a service
// of another app, as the node an instance of their service runs on: the
// caller sees which, not its serial, location, address or team. A manager
// sees everything.
func maskJoinedColumns(exprs []string, groups []string, isManager bool, masks ...joinedMask) ([]string, []any) {
	if isManager {
		return exprs, nil
	}
	clean := cleanGroups(groups)
	apps := "SELECT a.app FROM apps a JOIN apps_responsibles ar ON ar.app_id = a.id" +
		" JOIN auth_group ag ON ag.id = ar.group_id WHERE ag.role IN (" + Placeholders(len(clean)) + ")"
	out := make([]string, len(exprs))
	var args []any
	for i, expr := range exprs {
		out[i] = expr
		m := joinedColumnRe.FindStringSubmatch(strings.TrimSpace(expr))
		if m == nil {
			continue
		}
		for _, mask := range masks {
			if m[1] != mask.table || slices.Contains(mask.keep, m[2]) {
				continue
			}
			if len(clean) == 0 {
				out[i] = "NULL"
			} else {
				out[i] = "IF(" + mask.appCol + " IN (" + apps + "), " + expr + ", NULL)"
				args = append(args, stringsToAny(clean)...)
			}
		}
	}
	return out, args
}
