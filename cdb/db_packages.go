package cdb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/opensvc/oc3/schema"
)

func (oDb *DB) PurgePackagesOutdated(ctx context.Context, maxAge time.Duration) error {
	var query = `DELETE
		FROM packages
		WHERE
		  pkg_updated < DATE_SUB(NOW(), INTERVAL ? SECOND)`
	if count, err := oDb.execCountContext(ctx, query, maxAgeSeconds(maxAge)); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("packages")
	}
	return nil
}

func (oDb *DB) PurgePatchesOutdated(ctx context.Context, maxAge time.Duration) error {
	var query = `DELETE
		FROM patches
		WHERE
		  patch_updated < DATE_SUB(NOW(), INTERVAL ? SECOND)`
	if count, err := oDb.execCountContext(ctx, query, maxAgeSeconds(maxAge)); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("patches")
	}
	return nil
}

// buildPackagesQuery lists the packages installed on the nodes the user may see,
// as the historical packages table does: joined with nodes, whose app scopes the
// access, and with pkg_sig_provider, which names the signing key's provider.
func buildPackagesQuery(groups []string, isManager bool, selectExprs []string, filters []ColumnFilter) (string, []any, error) {
	// nodes is an inner join: a package of an unknown node is not listed, and the
	// access check filters on nodes.app. pkg_sig_provider is a left join: most
	// signatures have no known provider.
	q := From(schema.TPackages).
		Via(schema.TNodes).
		LeftJoin(schema.TPkgSigProvider).
		RawSelect(selectExprs...)

	if !isManager {
		cleanGroups := cleanGroups(groups)
		if len(cleanGroups) == 0 {
			q = q.WhereRaw("1=0")
		} else {
			args := make([]any, len(cleanGroups))
			for i, g := range cleanGroups {
				args[i] = g
			}
			q = q.WhereRaw(
				"nodes.app IN ("+
					"SELECT a.app FROM apps a"+
					" JOIN apps_responsibles ar ON ar.app_id = a.id"+
					" JOIN auth_group ag ON ag.id = ar.group_id"+
					" WHERE ag.role IN ("+Placeholders(len(cleanGroups))+")"+
					")",
				args...,
			)
		}
	} else {
		q = q.Where(schema.PackagesID, ">", 0)
	}

	// Column filters of the request, ANDed with the access control above.
	q = q.WhereFilters(filters)

	query, args, err := q.Build()
	if err != nil {
		return "", nil, fmt.Errorf("buildPackagesQuery: %w", err)
	}
	return query, args, nil
}

// GetPackages lists packages, ordered by node, name and architecture by default,
// as the historical packages table.
func (oDb *DB) GetPackages(ctx context.Context, p ListParams) ([]map[string]any, error) {
	query, args, err := buildPackagesQuery(p.Groups, p.IsManager, p.SelectExprs, p.Filters)
	if err != nil {
		return nil, err
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("nodes.nodename, packages.pkg_name, packages.pkg_arch")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getPackages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// PackageDiffNode is a node compared by a packages diff.
type PackageDiffNode struct {
	NodeID   string `json:"node_id"`
	Nodename string `json:"nodename"`
}

// PackageDiffRow is a package version installed on some of the compared nodes
// but not on all of them.
type PackageDiffRow struct {
	NodeID     string `json:"node_id"`
	PkgName    string `json:"pkg_name"`
	PkgVersion string `json:"pkg_version"`
	PkgArch    string `json:"pkg_arch"`
	PkgType    string `json:"pkg_type"`
}

// appsOfGroups is the condition restricting an app column to the apps one of the
// groups is responsible for, and its arguments; "1=0" without groups.
func appsOfGroups(appCol string, groups []string) (string, []any) {
	cleanGroups := cleanGroups(groups)
	if len(cleanGroups) == 0 {
		return "1=0", nil
	}
	args := make([]any, len(cleanGroups))
	for i, g := range cleanGroups {
		args[i] = g
	}
	return appCol + " IN (" +
		"SELECT a.app FROM apps a" +
		" JOIN apps_responsibles ar ON ar.app_id = a.id" +
		" JOIN auth_group ag ON ag.id = ar.group_id" +
		" WHERE ag.role IN (" + Placeholders(len(cleanGroups)) + ")" +
		")", args
}

// PackagesDiffNodes resolves the nodes a packages diff compares, as the
// historical lib_packages_diff: the given nodes, plus the nodes running the
// given services, or with encap the encapsulated nodes of those services (the
// nodes named after their instances' mon_vmname). Unless the user is a manager,
// only the nodes and services of the apps their groups are responsible for count.
// The nodes are returned by name.
func (oDb *DB) PackagesDiffNodes(ctx context.Context, nodeIDs, svcIDs []string, encap bool, groups []string, isManager bool) ([]PackageDiffNode, error) {
	var parts []string
	var args []any

	if len(nodeIDs) > 0 {
		part := "SELECT nodes.node_id FROM nodes WHERE nodes.node_id IN (" + Placeholders(len(nodeIDs)) + ")"
		for _, id := range nodeIDs {
			args = append(args, id)
		}
		if !isManager {
			cond, condArgs := appsOfGroups("nodes.app", groups)
			part += " AND " + cond
			args = append(args, condArgs...)
		}
		parts = append(parts, part)
	}

	if len(svcIDs) > 0 {
		var part string
		if encap {
			part = "SELECT nodes.node_id FROM svcmon" +
				" JOIN services ON services.svc_id = svcmon.svc_id" +
				" JOIN nodes ON nodes.nodename = svcmon.mon_vmname" +
				" WHERE svcmon.mon_vmname IS NOT NULL AND svcmon.mon_vmname != ''" +
				" AND svcmon.svc_id IN (" + Placeholders(len(svcIDs)) + ")"
		} else {
			part = "SELECT svcmon.node_id FROM svcmon" +
				" JOIN services ON services.svc_id = svcmon.svc_id" +
				" WHERE svcmon.svc_id IN (" + Placeholders(len(svcIDs)) + ")"
		}
		for _, id := range svcIDs {
			args = append(args, id)
		}
		if !isManager {
			cond, condArgs := appsOfGroups("services.svc_app", groups)
			part += " AND " + cond
			args = append(args, condArgs...)
		}
		parts = append(parts, part)
	}

	if len(parts) == 0 {
		return []PackageDiffNode{}, nil
	}
	query := "SELECT nodes.node_id, COALESCE(nodes.nodename, '') FROM nodes" +
		" WHERE nodes.node_id IN (" + strings.Join(parts, " UNION ") + ")" +
		" ORDER BY nodes.nodename, nodes.node_id"
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("packagesDiffNodes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	nodes := []PackageDiffNode{}
	for rows.Next() {
		var n PackageDiffNode
		if err := rows.Scan(&n.NodeID, &n.Nodename); err != nil {
			return nil, fmt.Errorf("packagesDiffNodes: %w", err)
		}
		nodes = append(nodes, n)
	}
	return nodes, rows.Err()
}

// PackagesDiff lists, on the given nodes, the package versions that are not
// installed on all of them: a package (name, version, architecture, type)
// present on every node is left out, as in the historical lib_packages_diff.
func (oDb *DB) PackagesDiff(ctx context.Context, nodeIDs []string) ([]PackageDiffRow, error) {
	if len(nodeIDs) < 2 {
		return []PackageDiffRow{}, nil
	}
	in := Placeholders(len(nodeIDs))
	query := "SELECT p.node_id, p.pkg_name, p.pkg_version, p.pkg_arch, COALESCE(p.pkg_type, '')" +
		" FROM packages p JOIN (" +
		"  SELECT pkg_name, pkg_version, pkg_arch, pkg_type FROM packages" +
		"  WHERE node_id IN (" + in + ")" +
		"  GROUP BY pkg_name, pkg_version, pkg_arch, pkg_type" +
		"  HAVING COUNT(DISTINCT node_id) != ?" +
		" ) u ON p.pkg_name = u.pkg_name AND p.pkg_version = u.pkg_version" +
		"  AND p.pkg_arch = u.pkg_arch AND p.pkg_type <=> u.pkg_type" +
		" WHERE p.node_id IN (" + in + ")" +
		" ORDER BY p.pkg_name, p.pkg_arch, p.pkg_type, p.pkg_version, p.node_id"
	args := make([]any, 0, 2*len(nodeIDs)+1)
	for _, id := range nodeIDs {
		args = append(args, id)
	}
	args = append(args, len(nodeIDs))
	for _, id := range nodeIDs {
		args = append(args, id)
	}
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("packagesDiff: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := []PackageDiffRow{}
	for rows.Next() {
		var r PackageDiffRow
		if err := rows.Scan(&r.NodeID, &r.PkgName, &r.PkgVersion, &r.PkgArch, &r.PkgType); err != nil {
			return nil, fmt.Errorf("packagesDiff: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
