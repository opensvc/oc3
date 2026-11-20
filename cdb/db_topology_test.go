package cdb

import (
	"context"
	"slices"
	"sort"
	"strings"
	"testing"
)

var topologyTestTables = []string{
	"CREATE TABLE nodes (node_id CHAR(36) PRIMARY KEY, nodename VARCHAR(255), model VARCHAR(255), os_name VARCHAR(255), asset_env VARCHAR(30)," +
		" app VARCHAR(64), loc_country VARCHAR(64), loc_city VARCHAR(64), loc_building VARCHAR(64), loc_room VARCHAR(64), loc_rack VARCHAR(64)," +
		" enclosure VARCHAR(64), hv VARCHAR(64), hvpool VARCHAR(64), hvvdc VARCHAR(64))",
	"CREATE TABLE services (svc_id CHAR(36) PRIMARY KEY, svcname VARCHAR(255), svc_env VARCHAR(10), svc_app VARCHAR(64), svc_availstatus VARCHAR(10), svc_status_updated DATETIME)",
	"CREATE TABLE svcmon (svc_id CHAR(36), node_id CHAR(36), mon_availstatus VARCHAR(10))",
	"CREATE TABLE resmon (id INT AUTO_INCREMENT PRIMARY KEY, svc_id CHAR(36), node_id CHAR(36), rid VARCHAR(255), res_type VARCHAR(64), res_status VARCHAR(10), res_disable TINYINT DEFAULT 0)",
	"CREATE TABLE svcdisks (id INT AUTO_INCREMENT PRIMARY KEY, disk_id VARCHAR(120), node_id CHAR(36), svc_id CHAR(36), disk_size INT)",
	"CREATE TABLE diskinfo (id INT AUTO_INCREMENT PRIMARY KEY, disk_id VARCHAR(120), disk_arrayid VARCHAR(60))",
	"CREATE TABLE apps (id INT AUTO_INCREMENT PRIMARY KEY, app VARCHAR(64))",
	"CREATE TABLE apps_responsibles (id INT AUTO_INCREMENT PRIMARY KEY, app_id INT, group_id INT)",
	"CREATE TABLE auth_group (id INT AUTO_INCREMENT PRIMARY KEY, role VARCHAR(255))",
	"INSERT INTO apps (id, app) VALUES (1, 'web'), (2, 'db')",
	"INSERT INTO auth_group (id, role) VALUES (1, 'WebTeam')",
	"INSERT INTO apps_responsibles (app_id, group_id) VALUES (1, 1)",
	// n1 and n2 in Paris, n1 on rack r1; n3 of app db, in Lyon
	"INSERT INTO nodes (node_id, nodename, model, os_name, asset_env, app, loc_country, loc_city, loc_rack) VALUES" +
		" ('n1', 'node1', 'm1', 'Linux', 'PRD', 'web', 'France', 'Paris', 'r1')," +
		" ('n2', 'node2', 'm1', 'Linux', 'PRD', 'web', 'France', 'Paris', NULL)," +
		" ('n3', 'node3', 'm2', 'Linux', 'DEV', 'db', 'France', 'Lyon', NULL)",
	"INSERT INTO services VALUES ('s1', 'websvc', 'PRD', 'web', 'up', NOW()), ('s2', 'dbsvc', 'DEV', 'db', 'down', NOW() - INTERVAL 1 HOUR)",
	"INSERT INTO svcmon VALUES ('s1', 'n1', 'up'), ('s1', 'n2', 'down'), ('s2', 'n2', 'up'), ('s2', 'n3', 'up')",
	"INSERT INTO resmon (svc_id, node_id, rid, res_type, res_status) VALUES" +
		" ('s1', 'n1', 'ip#0', 'ip', 'up'), ('s1', 'n1', 'fs#0', 'fs.flag', 'up'), ('s1', 'n1', 'fs#1', 'fs.flag', 'up')," +
		" ('s1', 'n2', 'ip#0', 'ip', 'down')",
	"INSERT INTO svcdisks (disk_id, node_id, svc_id, disk_size) VALUES ('d1', 'n1', 's1', 100), ('d2', 'n1', 's1', 200)",
	"INSERT INTO diskinfo (disk_id, disk_arrayid) VALUES ('d1', 'arr1'), ('d2', 'arr1')",
}

func topoIDs(t *Topology) []string {
	var l []string
	for _, v := range t.Vertices {
		l = append(l, v.ID)
	}
	sort.Strings(l)
	return l
}

func topoEdges(t *Topology) []string {
	var l []string
	for _, e := range t.Edges {
		a, b := e.From, e.To
		if b < a {
			a, b = b, a
		}
		s := a + " " + b
		if e.Status != "" {
			s += " " + e.Status
		}
		if e.Label != "" {
			s += " [" + e.Label + "]"
		}
		l = append(l, s)
	}
	sort.Strings(l)
	return l
}

func TestGetTopology(t *testing.T) {
	db := testMariaDB(t, topologyTestTables...)
	odb := New(db)
	ctx := context.Background()

	// From n1: its service s1, then the nodes of s1, n1 and n2; s2 of n2 not.
	topo, err := odb.GetTopology(ctx, TopologyRequest{NodeIDs: []string{"n1"}, Layers: []string{"nodes", "services", "cities", "countries"}, IsManager: true})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := topoIDs(topo), []string{"loc:cities:paris", "loc:countries:france", "node:n1", "node:n2", "svc:s1"}; !slices.Equal(got, want) {
		t.Errorf("vertices %v\nwant %v", got, want)
	}
	if got, want := topoEdges(topo), []string{
		"loc:cities:paris loc:countries:france",
		"loc:cities:paris node:n1", "loc:cities:paris node:n2",
		"node:n1 svc:s1 up", "node:n2 svc:s1 down",
	}; !slices.Equal(got, want) {
		t.Errorf("edges %v\nwant %v", got, want)
	}

	// From s2: n2 and n3, the services of nobody else; s2 not updated is undef.
	topo, err = odb.GetTopology(ctx, TopologyRequest{SvcIDs: []string{"s2"}, Layers: []string{"services", "apps", "envs"}, IsManager: true})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := topoIDs(topo), []string{"app:db", "env:dev", "svc:s2"}; !slices.Equal(got, want) {
		t.Errorf("vertices %v\nwant %v", got, want)
	}
	if topo.Vertices[slices.IndexFunc(topo.Vertices, func(v TopologyVertex) bool { return v.ID == "svc:s2" })].Status != "undef" {
		t.Errorf("s2 status: %+v", topo.Vertices)
	}

	// Resources replace the instance edges they detail; disks and arrays.
	topo, err = odb.GetTopology(ctx, TopologyRequest{SvcIDs: []string{"s1"}, Layers: []string{"nodes", "services", "resources", "disks", "arrays"}, IsManager: true})
	if err != nil {
		t.Fatal(err)
	}
	edges := topoEdges(topo)
	for _, want := range []string{
		"node:n1 res:s1:fs.flag up [2]", "node:n1 res:s1:ip up [1]", "node:n2 res:s1:ip down [1]",
		"res:s1:fs.flag svc:s1 up", "res:s1:ip svc:s1 up",
	} {
		if !slices.Contains(edges, want) {
			t.Errorf("no edge %q in %v", want, edges)
		}
	}
	if slices.Contains(edges, "node:n1 svc:s1 up") {
		t.Errorf("instance edge left with resources: %v", edges)
	}
	var disk TopologyVertex
	for _, v := range topo.Vertices {
		if v.Kind == "disk" {
			disk = v
		}
		if v.Kind == "fs" && v.Label != "fs.flag (2)" {
			t.Errorf("fs resource: %+v", v)
		}
	}
	if disk.Count != 2 || !strings.Contains(disk.Detail, "d1 100 MB") {
		t.Errorf("disk vertex %+v", disk)
	}
	for _, want := range []string{"array:arr1 " + disk.ID, disk.ID + " node:n1", disk.ID + " svc:s1"} {
		if !slices.Contains(edges, want) {
			t.Errorf("no edge %q in %v", want, edges)
		}
	}

	// A non manager starts from the objects of its apps only.
	topo, err = odb.GetTopology(ctx, TopologyRequest{NodeIDs: []string{"n3"}, SvcIDs: []string{"s1"}, Layers: []string{"nodes", "services"}, Groups: []string{"WebTeam"}})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := topoIDs(topo), []string{"node:n1", "node:n2", "svc:s1"}; !slices.Equal(got, want) {
		t.Errorf("non manager vertices %v\nwant %v", got, want)
	}
	// Nor do the expansions reach further: n2 hosts s2 of app db, run on n3
	// too, shown to a Manager only.
	topo, err = odb.GetTopology(ctx, TopologyRequest{NodeIDs: []string{"n2"}, Layers: []string{"nodes", "services"}, Groups: []string{"WebTeam"}})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := topoIDs(topo), []string{"node:n1", "node:n2", "svc:s1"}; !slices.Equal(got, want) {
		t.Errorf("non manager expansion %v\nwant %v", got, want)
	}
	topo, err = odb.GetTopology(ctx, TopologyRequest{NodeIDs: []string{"n2"}, Layers: []string{"nodes", "services"}, IsManager: true})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := topoIDs(topo), []string{"node:n1", "node:n2", "node:n3", "svc:s1", "svc:s2"}; !slices.Equal(got, want) {
		t.Errorf("manager expansion %v\nwant %v", got, want)
	}
	topo, err = odb.GetTopology(ctx, TopologyRequest{AppIDs: []int64{2}, Layers: []string{"nodes"}, Groups: []string{"WebTeam"}})
	if err != nil || len(topo.Vertices) != 0 {
		t.Errorf("app of another team: %v, %v", topo, err)
	}
}
