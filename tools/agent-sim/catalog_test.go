package main

import (
	"strings"
	"testing"
)

// TestRealisticCatalog checks the fleet of the realistic catalog: named by
// datacenter, of the apps of the catalog, its ids stable, its nodes described
// by their asset, and teardown removing them by id.
func TestRealisticCatalog(t *testing.T) {
	spec := fleetSpec{Prefix: "pa1", Seed: 1, Catalog: "realistic"}
	f, err := newFleet(spec)
	if err != nil {
		t.Fatal(err)
	}
	if f.nodeCount() != 33 || f.serviceCount() != 31 {
		t.Errorf("%d nodes, %d services", f.nodeCount(), f.serviceCount())
	}
	again, _ := newFleet(spec)
	names := map[string]bool{}
	for ci, c := range f.clusters {
		if c.app == "" || again.clusters[ci].id != c.id {
			t.Errorf("cluster %s: app %q, ids differ between builds", c.name, c.app)
		}
		for _, n := range c.nodes {
			if !strings.HasPrefix(n.name, "pa1-") && !strings.HasPrefix(n.name, "ly2-") || names[n.name] {
				t.Errorf("node name %q", n.name)
			}
			names[n.name] = true
			props := nodeAssetBody(spec, n)["properties"].(map[string]any)
			if props["model"].(map[string]any)["value"] == "Simulated VM" || props["loc_city"] == nil {
				t.Errorf("node %s asset %v", n.name, props["model"])
			}
			if pkgs := nodePackagesBody(spec, n)["package"].([]any); len(pkgs) < 50 {
				t.Errorf("node %s: %d packages", n.name, len(pkgs))
			}
		}
		for _, s := range c.services {
			if s.app != c.app || len(s.resources) == 0 || !strings.Contains(s.path, "/svc/") {
				t.Errorf("service %+v", s)
			}
		}
	}
	if !names["pa1-billing-db01"] || !names["ly2-kafka03"] {
		t.Errorf("names %v", names)
	}
	if _, err := newFleet(fleetSpec{Prefix: "x", Catalog: "unknown"}); err == nil {
		t.Error("an unknown catalog is refused")
	}
}
