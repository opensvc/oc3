package cdb

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"
)

// The topology of nodes, services and what relates them, as the historical
// collector's controllers/topo.py drew it with vis.js: a graph whose vertices
// are the objects of the layers asked for, and whose edges relate them. The
// graph is semantic: kinds and statuses, the client chooses the look.

// Topology layers.
const (
	TopoNodes     = "nodes"
	TopoServices  = "services"
	TopoApps      = "apps"
	TopoEnvs      = "envs"
	TopoResources = "resources"
	TopoDisks     = "disks"
	TopoArrays    = "arrays"
	TopoSAN       = "san"
)

// topoLocations are the location layers, from the closest to the node to the
// farthest, with their nodes column.
var topoLocations = []struct{ layer, column string }{
	{"hvs", "hv"},
	{"hvpools", "hvpool"},
	{"hvvdcs", "hvvdc"},
	{"enclosures", "enclosure"},
	{"racks", "loc_rack"},
	{"rooms", "loc_room"},
	{"buildings", "loc_building"},
	{"cities", "loc_city"},
	{"countries", "loc_country"},
}

// TopologyLayers are the layers a topology may show.
func TopologyLayers() []string {
	l := []string{TopoNodes, TopoServices, TopoApps, TopoEnvs, TopoResources, TopoDisks, TopoArrays, TopoSAN}
	for _, loc := range topoLocations {
		l = append(l, loc.layer)
	}
	return l
}

// MaxTopologyObjects bounds the nodes and services of a topology: more would
// draw nothing readable.
const MaxTopologyObjects = 1000

// ErrTopologyTooLarge is returned for a selection of more than
// MaxTopologyObjects nodes and services.
var ErrTopologyTooLarge = errors.New("topology too large")

// TopologyRequest is what a topology starts from, and what it shows.
type TopologyRequest struct {
	NodeIDs []string
	SvcIDs  []string
	AppIDs  []int64
	Layers  []string

	// The access of the caller: the starting objects are those of the apps its
	// groups are responsible for, unless manager.
	Groups    []string
	IsManager bool
}

// TopologyVertex is a vertex of a topology.
type TopologyVertex struct {
	ID string `json:"id"`
	// Kind is the layer of the vertex, but for a resource: its type family
	// (ip, fs, disk, container.docker, app, ...), and for a location: the layer.
	Kind   string `json:"kind"`
	Layer  string `json:"layer"`
	Label  string `json:"label"`
	Detail string `json:"detail,omitempty"`
	// Status is the availability of a service: up, down, warn, stdby up,
	// stdby down, n/a, or undef when not updated for 15 minutes.
	Status string `json:"status,omitempty"`
	NodeID string `json:"node_id,omitempty"`
	SvcID  string `json:"svc_id,omitempty"`
	Count  int    `json:"count,omitempty"`
}

// TopologyEdge relates two vertices. Status is the availability of the
// instance or of the resource it stands for, Label a count or a link speed.
type TopologyEdge struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Status string `json:"status,omitempty"`
	Label  string `json:"label,omitempty"`

	labels []string
}

// Topology is the graph of a TopologyRequest.
type Topology struct {
	Vertices []TopologyVertex `json:"vertices"`
	Edges    []TopologyEdge   `json:"edges"`

	index map[string]int
	edges map[[2]string]int
}

func (t *Topology) has(id string) bool {
	_, ok := t.index[id]
	return ok
}

func (t *Topology) addVertex(v TopologyVertex) {
	if t.has(v.ID) {
		return
	}
	t.index[v.ID] = len(t.Vertices)
	t.Vertices = append(t.Vertices, v)
}

// addEdge relates from and to, once whatever the direction. With multi, the
// labels of the next ones are counted on the first, as "2x8G + 1x4G".
func (t *Topology) addEdge(from, to, status, label string, multi bool) {
	if !t.has(from) || !t.has(to) || from == to {
		return
	}
	key := [2]string{from, to}
	if to < from {
		key = [2]string{to, from}
	}
	if i, ok := t.edges[key]; ok {
		if multi {
			e := &t.Edges[i]
			e.labels = append(e.labels, label)
			e.Label = countLabels(e.labels)
		}
		return
	}
	e := TopologyEdge{From: from, To: to, Status: status, Label: label}
	if multi {
		e.labels = []string{label}
		e.Label = countLabels(e.labels)
	}
	t.edges[key] = len(t.Edges)
	t.Edges = append(t.Edges, e)
}

func countLabels(labels []string) string {
	counts := map[string]int{}
	for _, l := range labels {
		if l != "" {
			counts[l]++
		}
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%dx%s", counts[k], k)
	}
	return strings.Join(parts, " + ")
}

// removeVertices drops the vertices of ids and their edges.
func (t *Topology) removeVertices(ids map[string]bool) {
	if len(ids) == 0 {
		return
	}
	vertices := t.Vertices[:0]
	for _, v := range t.Vertices {
		if !ids[v.ID] {
			vertices = append(vertices, v)
		}
	}
	t.Vertices = vertices
	edges := t.Edges[:0]
	for _, e := range t.Edges {
		if !ids[e.From] && !ids[e.To] {
			edges = append(edges, e)
		}
	}
	t.Edges = edges
}

type topoNode struct {
	id, name, model, osName, env, app string
	locs                              map[string]string
}

type topoService struct {
	id, name, env, app, status string
}

func topoID(kind string, parts ...string) string {
	return kind + ":" + strings.Join(parts, ":")
}

// appScope returns the condition keeping the rows whose app column is one the
// groups are responsible for, and its arguments; none for a manager.
func appScope(column string, groups []string, isManager bool) (string, []any) {
	if isManager {
		return "1=1", nil
	}
	clean := cleanGroups(groups)
	if len(clean) == 0 {
		return "1=0", nil
	}
	args := make([]any, len(clean))
	for i, g := range clean {
		args[i] = g
	}
	return column + " IN (SELECT a.app FROM apps a JOIN apps_responsibles ar ON ar.app_id = a.id" +
		" JOIN auth_group ag ON ag.id = ar.group_id WHERE ag.role IN (" + Placeholders(len(clean)) + "))", args
}

func stringArgs(l []string) []any {
	args := make([]any, len(l))
	for i, v := range l {
		args[i] = v
	}
	return args
}

func (oDb *DB) topoStrings(ctx context.Context, query string, args ...any) ([]string, error) {
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var l []string
	for rows.Next() {
		var s sql.NullString
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		if s.Valid && s.String != "" {
			l = append(l, s.String)
		}
	}
	return l, rows.Err()
}

func addUnique(set map[string]bool, l []string) {
	for _, v := range l {
		set[v] = true
	}
}

func topoSortedKeys(set map[string]bool) []string {
	l := make([]string, 0, len(set))
	for k := range set {
		l = append(l, k)
	}
	sort.Strings(l)
	return l
}

// topoObjects resolves the nodes and services of the topology: the starting
// ones the caller may see, then the services of those nodes, then the nodes of
// those services, as the historical collector, the expansions kept to the
// nodes and services the caller may see too: a node of the caller hosting an
// instance of a service of another app does not show that service, nor its
// other nodes.
func (oDb *DB) topoObjects(ctx context.Context, r TopologyRequest) (nodeIDs, svcIDs []string, err error) {
	nodes := map[string]bool{}
	svcs := map[string]bool{}
	svcScope, svcScopeArgs := appScope("services.svc_app", r.Groups, r.IsManager)
	nodeScope, nodeScopeArgs := appScope("nodes.app", r.Groups, r.IsManager)

	if len(r.AppIDs) > 0 {
		args := make([]any, len(r.AppIDs))
		for i, id := range r.AppIDs {
			args[i] = id
		}
		in := Placeholders(len(r.AppIDs))
		l, err := oDb.topoStrings(ctx, "SELECT services.svc_id FROM services JOIN apps ON apps.app = services.svc_app"+
			" WHERE apps.id IN ("+in+") AND "+svcScope, append(args, svcScopeArgs...)...)
		if err != nil {
			return nil, nil, fmt.Errorf("topology services of apps: %w", err)
		}
		addUnique(svcs, l)
		l, err = oDb.topoStrings(ctx, "SELECT nodes.node_id FROM nodes JOIN apps ON apps.app = nodes.app"+
			" WHERE apps.id IN ("+in+") AND "+nodeScope, append(args, nodeScopeArgs...)...)
		if err != nil {
			return nil, nil, fmt.Errorf("topology nodes of apps: %w", err)
		}
		addUnique(nodes, l)
	}
	if len(r.SvcIDs) > 0 {
		l, err := oDb.topoStrings(ctx, "SELECT services.svc_id FROM services WHERE services.svc_id IN ("+
			Placeholders(len(r.SvcIDs))+") AND "+svcScope, append(stringArgs(r.SvcIDs), svcScopeArgs...)...)
		if err != nil {
			return nil, nil, fmt.Errorf("topology services: %w", err)
		}
		addUnique(svcs, l)
	}
	if len(r.NodeIDs) > 0 {
		l, err := oDb.topoStrings(ctx, "SELECT nodes.node_id FROM nodes WHERE nodes.node_id IN ("+
			Placeholders(len(r.NodeIDs))+") AND "+nodeScope, append(stringArgs(r.NodeIDs), nodeScopeArgs...)...)
		if err != nil {
			return nil, nil, fmt.Errorf("topology nodes: %w", err)
		}
		addUnique(nodes, l)
	}
	if len(nodes) > MaxTopologyObjects || len(svcs) > MaxTopologyObjects {
		return nil, nil, ErrTopologyTooLarge
	}
	if len(nodes) > 0 {
		l, err := oDb.topoStrings(ctx, "SELECT DISTINCT svcmon.svc_id FROM svcmon JOIN services ON services.svc_id = svcmon.svc_id"+
			" WHERE svcmon.node_id IN ("+Placeholders(len(nodes))+") AND "+svcScope,
			append(stringArgs(topoSortedKeys(nodes)), svcScopeArgs...)...)
		if err != nil {
			return nil, nil, fmt.Errorf("topology services of the nodes: %w", err)
		}
		addUnique(svcs, l)
	}
	if len(svcs) > MaxTopologyObjects {
		return nil, nil, ErrTopologyTooLarge
	}
	if len(svcs) > 0 {
		l, err := oDb.topoStrings(ctx, "SELECT DISTINCT svcmon.node_id FROM svcmon JOIN nodes ON nodes.node_id = svcmon.node_id"+
			" WHERE svcmon.svc_id IN ("+Placeholders(len(svcs))+") AND "+nodeScope,
			append(stringArgs(topoSortedKeys(svcs)), nodeScopeArgs...)...)
		if err != nil {
			return nil, nil, fmt.Errorf("topology nodes of the services: %w", err)
		}
		addUnique(nodes, l)
	}
	if len(nodes)+len(svcs) > MaxTopologyObjects {
		return nil, nil, ErrTopologyTooLarge
	}
	return topoSortedKeys(nodes), topoSortedKeys(svcs), nil
}

// GetTopology returns the topology r asks for.
func (oDb *DB) GetTopology(ctx context.Context, r TopologyRequest) (*Topology, error) {
	defer logDuration("GetTopology", time.Now())
	show := map[string]bool{}
	for _, l := range r.Layers {
		show[l] = true
	}
	var locs []struct{ layer, column string }
	for _, loc := range topoLocations {
		if show[loc.layer] {
			locs = append(locs, loc)
		}
	}
	t := &Topology{Vertices: []TopologyVertex{}, Edges: []TopologyEdge{}, index: map[string]int{}, edges: map[[2]string]int{}}

	nodeIDs, svcIDs, err := oDb.topoObjects(ctx, r)
	if err != nil {
		return nil, err
	}
	if len(nodeIDs)+len(svcIDs) == 0 {
		return t, nil
	}

	// The nodes, for their own vertices or their locations; the instances,
	// which relate them to the services.
	nodes := map[string]*topoNode{}
	type instanceKey struct{ node, svc string }
	instances := map[instanceKey]string{}
	if show[TopoNodes] || len(locs) > 0 {
		if nodes, err = oDb.topoNodes(ctx, nodeIDs); err != nil {
			return nil, err
		}
		if len(svcIDs) > 0 {
			rows, err := oDb.DB.QueryContext(ctx, "SELECT node_id, svc_id, COALESCE(mon_availstatus, '') FROM svcmon WHERE svc_id IN ("+
				Placeholders(len(svcIDs))+")", stringArgs(svcIDs)...)
			if err != nil {
				return nil, fmt.Errorf("topology instances: %w", err)
			}
			for rows.Next() {
				var k instanceKey
				var status string
				if err := rows.Scan(&k.node, &k.svc, &status); err != nil {
					_ = rows.Close()
					return nil, fmt.Errorf("topology instances: %w", err)
				}
				if _, ok := instances[k]; !ok {
					instances[k] = status
				}
			}
			_ = rows.Close()
		}
	}
	instanceKeys := make([]instanceKey, 0, len(instances))
	for k := range instances {
		instanceKeys = append(instanceKeys, k)
	}
	sort.Slice(instanceKeys, func(i, j int) bool {
		if instanceKeys[i].node != instanceKeys[j].node {
			return instanceKeys[i].node < instanceKeys[j].node
		}
		return instanceKeys[i].svc < instanceKeys[j].svc
	})

	// The resources by node, service and type, their count by status.
	type resKey struct{ node, svc, resType, status string }
	resCounts := map[resKey]int{}
	var resKeys []resKey
	if show[TopoResources] && len(svcIDs) > 0 && len(nodeIDs) > 0 {
		rows, err := oDb.DB.QueryContext(ctx, "SELECT node_id, svc_id, COALESCE(res_type, ''), COALESCE(res_status, ''), COUNT(*) FROM resmon"+
			" WHERE svc_id IN ("+Placeholders(len(svcIDs))+") AND node_id IN ("+Placeholders(len(nodeIDs))+")"+
			" AND (res_disable = 0 OR res_disable IS NULL) GROUP BY node_id, svc_id, res_type, res_status ORDER BY node_id, svc_id, res_type, res_status",
			append(stringArgs(svcIDs), stringArgs(nodeIDs)...)...)
		if err != nil {
			return nil, fmt.Errorf("topology resources: %w", err)
		}
		for rows.Next() {
			var k resKey
			var n int
			if err := rows.Scan(&k.node, &k.svc, &k.resType, &k.status, &n); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("topology resources: %w", err)
			}
			if k.resType == "" {
				continue
			}
			resCounts[k] = n
			resKeys = append(resKeys, k)
		}
		_ = rows.Close()
	}
	nodeSvcHasResources := map[instanceKey]bool{}
	for _, k := range resKeys {
		nodeSvcHasResources[instanceKey{k.node, k.svc}] = true
	}

	// The disks by node, service and array.
	type diskRow struct {
		id   string
		size int64
	}
	type diskKey struct{ node, svc, array string }
	disks := map[diskKey][]diskRow{}
	var diskKeys []diskKey
	if (show[TopoDisks] || show[TopoArrays]) && len(svcIDs) > 0 && len(nodeIDs) > 0 {
		rows, err := oDb.DB.QueryContext(ctx, "SELECT d.node_id, d.svc_id, COALESCE(i.disk_arrayid, ''), d.disk_id, COALESCE(d.disk_size, 0)"+
			" FROM svcdisks d LEFT JOIN diskinfo i ON i.disk_id = d.disk_id"+
			" WHERE d.svc_id IN ("+Placeholders(len(svcIDs))+") AND d.node_id IN ("+Placeholders(len(nodeIDs))+")"+
			" ORDER BY d.node_id, d.svc_id, i.disk_arrayid, d.disk_id",
			append(stringArgs(svcIDs), stringArgs(nodeIDs)...)...)
		if err != nil {
			return nil, fmt.Errorf("topology disks: %w", err)
		}
		for rows.Next() {
			var k diskKey
			var d diskRow
			if err := rows.Scan(&k.node, &k.svc, &k.array, &d.id, &d.size); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("topology disks: %w", err)
			}
			if _, ok := disks[k]; !ok {
				diskKeys = append(diskKeys, k)
			}
			disks[k] = append(disks[k], d)
		}
		_ = rows.Close()
	}

	services := map[string]*topoService{}
	if show[TopoServices] || show[TopoApps] || show[TopoEnvs] || show[TopoResources] || show[TopoDisks] {
		if services, err = oDb.topoServices(ctx, svcIDs); err != nil {
			return nil, err
		}
	}
	svcName := func(svcID string) string {
		if s, ok := services[svcID]; ok && s.name != "" {
			return s.name
		}
		return svcID
	}
	nodeIsShown := func(id string) bool { return t.has(topoID("node", id)) }

	// Nodes and services.
	if show[TopoNodes] {
		for _, id := range nodeIDs {
			n, ok := nodes[id]
			if !ok {
				continue
			}
			detail := strings.Trim(strings.Join([]string{n.osName, n.model}, ", "), ", ")
			t.addVertex(TopologyVertex{ID: topoID("node", id), Kind: "node", Layer: TopoNodes, Label: n.name, Detail: detail, NodeID: id})
		}
	}
	if show[TopoServices] {
		for _, id := range svcIDs {
			s, ok := services[id]
			if !ok {
				continue
			}
			t.addVertex(TopologyVertex{ID: topoID("svc", id), Kind: "service", Layer: TopoServices, Label: svcName(id), Status: s.status, SvcID: id})
		}
		if show[TopoNodes] {
			for _, k := range instanceKeys {
				if show[TopoResources] && nodeSvcHasResources[k] {
					continue
				}
				t.addEdge(topoID("node", k.node), topoID("svc", k.svc), instances[k], "", false)
			}
		}
	}

	// Arrays and disks.
	diskVertex := func(k diskKey) string {
		h := md5.New()
		for _, d := range disks[k] {
			fmt.Fprintf(h, "%s|%s|%d|%s\n", k.node, k.svc, d.size, d.id)
		}
		return topoID("disk", hex.EncodeToString(h.Sum(nil)))
	}
	if show[TopoArrays] {
		for _, k := range diskKeys {
			if k.array == "" || slices.Contains(nodeIDs, k.array) {
				continue
			}
			t.addVertex(TopologyVertex{ID: topoID("array", k.array), Kind: "array", Layer: TopoArrays, Label: k.array})
		}
	}
	if show[TopoDisks] {
		for _, k := range diskKeys {
			rows := disks[k]
			var total int64
			for _, d := range rows {
				total += d.size
			}
			detail := ""
			if len(rows) > 3 {
				detail = fmt.Sprintf("%d disks, %d MB", len(rows), total)
			} else {
				ids := make([]string, len(rows))
				for i, d := range rows {
					ids[i] = fmt.Sprintf("%s %d MB", d.id, d.size)
				}
				detail = strings.Join(ids, "\n")
			}
			t.addVertex(TopologyVertex{ID: diskVertex(k), Kind: "disk", Layer: TopoDisks, Label: svcName(k.svc), Detail: detail, SvcID: k.svc, NodeID: k.node, Count: len(rows)})
		}
		for _, k := range diskKeys {
			disk := diskVertex(k)
			if show[TopoArrays] {
				t.addEdge(topoID("array", k.array), disk, "", "", false)
			}
			if show[TopoNodes] {
				t.addEdge(topoID("node", k.node), disk, "", "", false)
			}
			if show[TopoServices] {
				t.addEdge(topoID("svc", k.svc), disk, "", "", false)
			}
		}
	}

	// Locations: one vertex per value, each related to the next farther one
	// its nodes have, and the nodes, or else the services, to the closest.
	if len(locs) > 0 {
		for _, id := range nodeIDs {
			n, ok := nodes[id]
			if !ok {
				continue
			}
			for _, loc := range locs {
				if v := n.locs[loc.column]; v != "" {
					t.addVertex(TopologyVertex{ID: topoID("loc", loc.layer, strings.ToLower(v)), Kind: loc.layer, Layer: loc.layer, Label: v})
				}
			}
			var chain []string
			for _, loc := range locs {
				if v := n.locs[loc.column]; v != "" {
					chain = append(chain, topoID("loc", loc.layer, strings.ToLower(v)))
				}
			}
			for i := 0; i+1 < len(chain); i++ {
				t.addEdge(chain[i], chain[i+1], "", "", false)
			}
		}
		closest := func(nodeID string) string {
			n, ok := nodes[nodeID]
			if !ok {
				return ""
			}
			for _, loc := range locs {
				if v := n.locs[loc.column]; v != "" {
					return topoID("loc", loc.layer, strings.ToLower(v))
				}
			}
			return ""
		}
		if show[TopoNodes] {
			for _, id := range nodeIDs {
				if loc := closest(id); loc != "" {
					t.addEdge(topoID("node", id), loc, "", "", false)
				}
			}
		} else if show[TopoServices] {
			for _, k := range instanceKeys {
				if loc := closest(k.node); loc != "" {
					t.addEdge(topoID("svc", k.svc), loc, instances[k], "", false)
				}
			}
		}
	}

	// Environments and applications, of the services, or else of the nodes.
	for _, layer := range []string{TopoEnvs, TopoApps} {
		if !show[layer] {
			continue
		}
		kind := strings.TrimSuffix(layer, "s")
		value := func(env, app string) string {
			v := env
			if layer == TopoApps {
				v = app
			}
			if v == "" {
				return "unknown"
			}
			return v
		}
		relate := func(from, v string) {
			id := topoID(kind, strings.ToLower(v))
			t.addVertex(TopologyVertex{ID: id, Kind: kind, Layer: layer, Label: v})
			t.addEdge(from, id, "", "", false)
		}
		if show[TopoServices] {
			for _, id := range svcIDs {
				if s, ok := services[id]; ok {
					relate(topoID("svc", id), value(s.env, s.app))
				}
			}
		} else if show[TopoNodes] {
			for _, id := range nodeIDs {
				if n, ok := nodes[id]; ok && nodeIsShown(id) {
					relate(topoID("node", id), value(n.env, n.app))
				}
			}
		}
	}

	// Resources, by service and type family, related to their service and the
	// nodes running them.
	if show[TopoResources] {
		type svcType struct{ svc, resType string }
		counts := map[svcType]int{}
		var order []svcType
		for _, k := range resKeys {
			st := svcType{k.svc, k.resType}
			if _, ok := counts[st]; !ok {
				order = append(order, st)
			}
			counts[st] += resCounts[k]
		}
		resID := func(st svcType) string { return topoID("res", st.svc, st.resType) }
		for _, st := range order {
			kind := st.resType
			switch st.resType {
			case "container.docker", "ip", "disk.scsireserv", "disk", "fs":
			default:
				kind, _, _ = strings.Cut(st.resType, ".")
			}
			t.addVertex(TopologyVertex{ID: resID(st), Kind: kind, Layer: TopoResources,
				Label: fmt.Sprintf("%s (%d)", st.resType, counts[st]), SvcID: st.svc, Count: counts[st]})
		}
		nodeCounts := map[resKey]int{}
		for _, k := range resKeys {
			nodeCounts[resKey{k.node, k.svc, k.resType, ""}] += resCounts[k]
		}
		for _, k := range resKeys {
			st := svcType{k.svc, k.resType}
			t.addEdge(topoID("svc", k.svc), resID(st), k.status, "", false)
			if show[TopoServices] && show[TopoNodes] {
				n := nodeCounts[resKey{k.node, k.svc, k.resType, ""}]
				t.addEdge(topoID("node", k.node), resID(st), k.status, fmt.Sprint(n), false)
			}
		}
	}

	if show[TopoSAN] {
		if err := oDb.topoSAN(ctx, t, nodeIDs); err != nil {
			return nil, err
		}
	}
	return t, nil
}

func (oDb *DB) topoNodes(ctx context.Context, nodeIDs []string) (map[string]*topoNode, error) {
	m := map[string]*topoNode{}
	if len(nodeIDs) == 0 {
		return m, nil
	}
	cols := []string{"node_id", "nodename", "model", "os_name", "asset_env", "app"}
	for _, loc := range topoLocations {
		cols = append(cols, loc.column)
	}
	sel := make([]string, len(cols))
	for i, c := range cols {
		sel[i] = "COALESCE(" + c + ", '')"
	}
	rows, err := oDb.DB.QueryContext(ctx, "SELECT "+strings.Join(sel, ", ")+" FROM nodes WHERE node_id IN ("+
		Placeholders(len(nodeIDs))+")", stringArgs(nodeIDs)...)
	if err != nil {
		return nil, fmt.Errorf("topology nodes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		values := make([]string, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("topology nodes: %w", err)
		}
		n := &topoNode{id: values[0], name: values[1], model: values[2], osName: values[3], env: values[4], app: values[5], locs: map[string]string{}}
		for i, loc := range topoLocations {
			v := strings.TrimSpace(values[6+i])
			if v != "" && v != "Unknown" {
				n.locs[loc.column] = v
			}
		}
		m[n.id] = n
	}
	return m, rows.Err()
}

func (oDb *DB) topoServices(ctx context.Context, svcIDs []string) (map[string]*topoService, error) {
	m := map[string]*topoService{}
	if len(svcIDs) == 0 {
		return m, nil
	}
	rows, err := oDb.DB.QueryContext(ctx, "SELECT svc_id, COALESCE(svcname, ''), COALESCE(svc_env, ''), COALESCE(svc_app, ''),"+
		" IF(svc_status_updated IS NULL OR svc_status_updated < NOW() - INTERVAL 15 MINUTE, 'undef', COALESCE(svc_availstatus, 'undef'))"+
		" FROM services WHERE svc_id IN ("+Placeholders(len(svcIDs))+")", stringArgs(svcIDs)...)
	if err != nil {
		return nil, fmt.Errorf("topology services: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		s := &topoService{}
		if err := rows.Scan(&s.id, &s.name, &s.env, &s.app, &s.status); err != nil {
			return nil, fmt.Errorf("topology services: %w", err)
		}
		m[s.id] = s
	}
	return m, rows.Err()
}

// topoSAN adds the SAN switches between the nodes and the arrays: their links,
// the links of the node HBAs and of the array targets, by speed, and then
// drops the switches relating less than two of these.
func (oDb *DB) topoSAN(ctx context.Context, t *Topology, nodeIDs []string) error {
	if len(nodeIDs) == 0 {
		return nil
	}
	in := Placeholders(len(nodeIDs))
	args := stringArgs(nodeIDs)
	hbaIDs, err := oDb.topoStrings(ctx, "SELECT hba_id FROM node_hba WHERE node_id IN ("+in+")", args...)
	if err != nil {
		return fmt.Errorf("topology hbas: %w", err)
	}
	tgtIDs, err := oDb.topoStrings(ctx, "SELECT tgt_id FROM stor_zone WHERE node_id IN ("+in+")", args...)
	if err != nil {
		return fmt.Errorf("topology targets: %w", err)
	}
	ends := append(hbaIDs, tgtIDs...)
	if len(ends) == 0 {
		return nil
	}
	edge, err := oDb.topoStrings(ctx, "SELECT DISTINCT sw_name FROM switches WHERE sw_rportname IN ("+Placeholders(len(ends))+")", stringArgs(ends)...)
	if err != nil {
		return fmt.Errorf("topology edge switches: %w", err)
	}
	if len(edge) == 0 {
		return nil
	}

	type port struct{ sw, name, remote, speed string }
	rows, err := oDb.DB.QueryContext(ctx, "SELECT COALESCE(sw_name, ''), COALESCE(sw_portname, ''), COALESCE(sw_rportname, ''), COALESCE(sw_portspeed, '')"+
		" FROM switches ORDER BY sw_name, sw_index")
	if err != nil {
		return fmt.Errorf("topology switch ports: %w", err)
	}
	var ports []port
	for rows.Next() {
		var p port
		if err := rows.Scan(&p.sw, &p.name, &p.remote, &p.speed); err != nil {
			_ = rows.Close()
			return fmt.Errorf("topology switch ports: %w", err)
		}
		ports = append(ports, p)
	}
	_ = rows.Close()
	byPort := map[string]port{}
	byRemote := map[string]port{}
	var switches []string
	for _, p := range ports {
		id := topoID("sansw", p.sw)
		if !t.has(id) {
			switches = append(switches, id)
			t.addVertex(TopologyVertex{ID: id, Kind: "sansw", Layer: TopoSAN, Label: p.sw})
		}
		if p.name != "" {
			byPort[p.name] = p
		}
		if p.remote != "" {
			byRemote[p.remote] = p
		}
	}
	// Inter-switch links.
	for _, p := range ports {
		a, okA := byPort[p.name]
		b, okB := byPort[p.remote]
		if !okA || !okB || p.name == "" || p.remote == "" {
			continue
		}
		t.addEdge(topoID("sansw", a.sw), topoID("sansw", b.sw), "", p.speed, true)
	}
	// Node HBAs.
	rows, err = oDb.DB.QueryContext(ctx, "SELECT node_id, COALESCE(hba_id, ''), COALESCE(hba_type, '') FROM node_hba WHERE node_id IN ("+in+") ORDER BY node_id, hba_id", args...)
	if err != nil {
		return fmt.Errorf("topology hbas: %w", err)
	}
	for rows.Next() {
		var nodeID, hbaID, hbaType string
		if err := rows.Scan(&nodeID, &hbaID, &hbaType); err != nil {
			_ = rows.Close()
			return fmt.Errorf("topology hbas: %w", err)
		}
		if p, ok := byRemote[hbaID]; ok {
			t.addEdge(topoID("node", nodeID), topoID("sansw", p.sw), "", hbaType+"/"+p.speed, true)
		}
	}
	_ = rows.Close()
	// Array targets.
	rows, err = oDb.DB.QueryContext(ctx, "SELECT DISTINCT z.tgt_id, COALESCE(a.array_name, '') FROM stor_zone z"+
		" JOIN stor_array_tgtid t ON t.array_tgtid = z.tgt_id JOIN stor_array a ON a.id = t.array_id"+
		" WHERE z.node_id IN ("+in+") ORDER BY z.tgt_id", args...)
	if err != nil {
		return fmt.Errorf("topology array targets: %w", err)
	}
	for rows.Next() {
		var tgtID, arrayName string
		if err := rows.Scan(&tgtID, &arrayName); err != nil {
			_ = rows.Close()
			return fmt.Errorf("topology array targets: %w", err)
		}
		if p, ok := byRemote[tgtID]; ok {
			t.addEdge(topoID("array", arrayName), topoID("sansw", p.sw), "", p.speed, true)
		}
	}
	_ = rows.Close()

	// Keep the switches relating two vertices or more.
	related := map[string]map[string]bool{}
	for _, e := range t.Edges {
		for _, pair := range [][2]string{{e.From, e.To}, {e.To, e.From}} {
			if strings.HasPrefix(pair[0], "sansw:") {
				if related[pair[0]] == nil {
					related[pair[0]] = map[string]bool{}
				}
				related[pair[0]][pair[1]] = true
			}
		}
	}
	drop := map[string]bool{}
	for _, sw := range switches {
		if len(related[sw]) < 2 {
			drop[sw] = true
		}
	}
	t.removeVertices(drop)
	return nil
}
