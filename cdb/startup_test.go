package cdb

import (
	"slices"
	"strings"
	"testing"
)

func startupSequence(s *Startup) []string {
	var l []string
	for _, e := range s.Edges {
		if e.Kind == "sequence" {
			l = append(l, e.From+" > "+e.To)
		}
	}
	return l
}

func startupHasEdge(s *Startup, from, to, kind string) bool {
	return slices.ContainsFunc(s.Edges, func(e StartupEdge) bool {
		return e.From == from && e.To == to && e.Kind == kind
	})
}

func TestBuildStartupOrder(t *testing.T) {
	// The haproxy service of dev6: the ips linked to container#0 start
	// right after it, the volume before the containers, the task not at all.
	config := `[DEFAULT]
nodes = *
id = 5a6d6eef

[volume#1]
type = shm
install = ` + "`" + `	/haproxy/haproxy.cfg from ./cfg/{name}
	/haproxy/certs/` + "`" + `

[ip#1]
type = netns
netns = container#0

[ip#2]
type = netns
netns = container#0

[container#0]
image = ghcr.io/opensvc/pause

[container#1]
image = haproxy
netns = container#0

[task#acme]
type = acme
`
	states := map[string]StartupResourceState{
		"container#0": {Status: "up", Type: "container.docker", Desc: "docker pause"},
		"ip#1":        {Status: "up", Type: "ip.netns"},
		"volume#1":    {Status: "down", Type: "volume"},
	}
	s := BuildStartup(config, "system/svc/haproxy", "dev6n1", states, nil, false)
	want := []string{
		"service > node",
		"node > resource:volume#1",
		"resource:volume#1 > resource:container#0",
		"resource:container#0 > resource:ip#1",
		"resource:ip#1 > resource:ip#2",
		"resource:ip#2 > resource:container#1",
	}
	if got := startupSequence(s); !slices.Equal(got, want) {
		t.Fatalf("sequence:\n got %v\nwant %v", got, want)
	}
	for _, v := range s.Vertices {
		switch v.ID {
		case "resource:container#0":
			if v.Status != "up" || v.Type != "docker" || v.Family != "container" || v.Desc != "docker pause" {
				t.Errorf("container#0: %+v", v)
			}
		case "resource:ip#2":
			if v.Status != "" || v.Type != "netns" {
				t.Errorf("ip#2, not reported: %+v", v)
			}
		case "resource:task#acme":
			t.Errorf("a task is drawn")
		}
	}
}

func TestBuildStartupSubsets(t *testing.T) {
	config := `[DEFAULT]
nodes = n1 n2
pre_start = /bin/true svc

[subset#app:web]
parallel = true

[app#b]
subset = web

[app#a]
subset = web
pre_start = /bin/echo a

[app#z]
start = 10

[app#y]
start = 20

[app#db]
subset = db

[fs#2]
[fs#10]
`
	s := BuildStartup(config, "svc", "n1", nil, nil, false)
	want := []string{
		"service > node",
		"node > trigger:service:pre_start",
		"trigger:service:pre_start > resource:fs#10",
		"resource:fs#10 > resource:fs#2",
		// The resources of no subset, by start sequence number.
		"resource:fs#2 > resource:app#z",
		"resource:app#z > resource:app#y",
		// Then the subsets by name, db in series, web in parallel.
		"resource:app#y > subset:app:db",
		"subset:app:db > resource:app#db",
		"resource:app#db > subset:app:web",
		"subset:app:web > trigger:app#a:pre_start",
		"trigger:app#a:pre_start > resource:app#a",
		"subset:app:web > resource:app#b",
	}
	if got := startupSequence(s); !slices.Equal(got, want) {
		t.Fatalf("sequence:\n got %v\nwant %v", got, want)
	}
	for _, v := range s.Vertices {
		if v.ID == "subset:app:web" && !v.Parallel {
			t.Errorf("subset web not parallel")
		}
		if v.ID == "subset:app:db" && v.Parallel {
			t.Errorf("subset db parallel")
		}
	}
}

func TestBuildStartupScopesAndDisabled(t *testing.T) {
	config := `[DEFAULT]
nodes = n1
drpnodes = d1

[fs#1]
disable@drpnodes = true

[fs#2]
disable_on = nodes

[ip#1]
start_requires = fs#1 fs#2(up,stdby up)
`
	s := BuildStartup(config, "svc", "d1", nil, nil, false)
	if got := startupSequence(s); !slices.Equal(got, []string{"service > node", "node > resource:ip#1", "resource:ip#1 > resource:fs#2"}) {
		t.Fatalf("drp node sequence: %v", got)
	}
	if !startupHasEdge(s, "resource:fs#2", "resource:ip#1", "requires") {
		t.Errorf("no requires edge fs#2 > ip#1: %v", s.Edges)
	}
	for _, e := range s.Edges {
		if e.Kind == "requires" && e.Label != "up,stdby up" {
			t.Errorf("requires label %q", e.Label)
		}
	}
	s = BuildStartup(config, "svc", "n1", nil, nil, true)
	disabled := map[string]bool{}
	for _, v := range s.Vertices {
		disabled[v.ID] = v.Disabled
	}
	if disabled["resource:fs#1"] || !disabled["resource:fs#2"] {
		t.Errorf("disabled on n1: %v", disabled)
	}
}

func TestBuildStartupEncap(t *testing.T) {
	config := `[DEFAULT]
nodes = n1
encapnodes = vm1

[container#1]
type = kvm
name = vm1

[app#1]
encap = true

[fs#1]
tags = encap
`
	encap := map[string]map[string]StartupResourceState{"vm1": {"app#1": {Status: "up"}}}
	s := BuildStartup(config, "svc", "n1", nil, encap, false)
	want := []string{
		"service > node",
		"node > resource:container#1",
		"resource:container#1 > encap:vm1",
		"encap:vm1 > resource:vm1:fs#1",
		"resource:vm1:fs#1 > resource:vm1:app#1",
	}
	if got := startupSequence(s); !slices.Equal(got, want) {
		t.Fatalf("sequence:\n got %v\nwant %v", got, want)
	}
	for _, v := range s.Vertices {
		if v.ID == "resource:vm1:app#1" && (v.Status != "up" || v.Node != "vm1") {
			t.Errorf("encap app: %+v", v)
		}
	}
}

func TestParseSvcConfigV2Escaped(t *testing.T) {
	c := parseSvcConfig(`[DEFAULT]\nnodes = n1\n[fs#1]\nmnt = /a\n  continued`)
	if v, _ := c.raw("fs#1", "mnt"); v != "/a\ncontinued" {
		t.Errorf("mnt %q", v)
	}
	if !slices.Equal(c.sections, []string{"DEFAULT", "fs#1"}) {
		t.Errorf("sections %v", c.sections)
	}
}

func TestParseStartRequires(t *testing.T) {
	got := parseStartRequires(strings.Fields("ip#0 fs#0(down,stdby down) app#1(up)"))
	want := []startRequirement{{"ip#0", ""}, {"fs#0", "down,stdby down"}, {"app#1", "up"}}
	if !slices.Equal(got, want) {
		t.Errorf("got %v", got)
	}
}
