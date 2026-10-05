package serverhandlers

import (
	"fmt"
	"strings"
	"testing"

	"github.com/opensvc/oc3/cdb"
)

func TestSanTopology(t *testing.T) {
	// One fabric: the adapter on an edge switch, a trunk of two links to a core
	// switch holding the array ports, and a second trunk to a switch leading nowhere.
	ports := []cdb.SwitchPort{
		{Name: "edge", Fabric: "fa", WWN: "e1", Remote: "hba0", Type: "F-Port", Index: 0, Speed: 16},
		{Name: "edge", Fabric: "fa", WWN: "e1", Remote: "other", Type: "F-Port", Index: 1, Speed: 16},
		{Name: "edge", Fabric: "fa", WWN: "e1", Remote: "c1", Type: "E-Port", Index: 46, Speed: 32},
		{Name: "edge", Fabric: "fa", WWN: "e1", Remote: "c1", Type: "E-Port", Index: 47, Speed: 32},
		{Name: "edge", Fabric: "fa", WWN: "e1", Remote: "d1", Type: "E-Port", Index: 40, Speed: 8},
		{Name: "core", Fabric: "fa", WWN: "c1", Remote: "e1", Type: "E-Port", Index: 0, Speed: 32},
		{Name: "core", Fabric: "fa", WWN: "c1", Remote: "e1", Type: "E-Port", Index: 1, Speed: 32},
		{Name: "core", Fabric: "fa", WWN: "c1", Remote: "tgt0", Type: "F-Port", Index: 16, Speed: 32},
		{Name: "core", Fabric: "fa", WWN: "c1", Remote: "tgt1", Type: "F-Port", Index: 17, Speed: 32},
		{Name: "core", Fabric: "fa", WWN: "c1", Remote: "tgt9", Type: "F-Port", Index: 18, Speed: 32},
		{Name: "dead", Fabric: "fa", WWN: "d1", Remote: "e1", Type: "E-Port", Index: 0, Speed: 8},
	}
	endpoints := []cdb.SANEndpoint{
		{HBA: "hba0", Target: "tgt0", Array: "vsp"},
		{HBA: "hba0", Target: "tgt1", Array: "vsp"},
		{HBA: "hba1"},
	}
	g := sanTopology("n1", "node1", endpoints, ports)

	var nodes []string
	for _, n := range g.Nodes {
		nodes = append(nodes, fmt.Sprintf("%s/%d[%s]", n.Id, n.Rank, strings.Join(n.Ports, " ")))
	}
	wantNodes := "server:n1/0[hba0] switch:e1/1[0 46,47] switch:c1/2[0,1 16 17] array:vsp/3[tgt0 tgt1]"
	if got := strings.Join(nodes, " "); got != wantNodes {
		t.Errorf("nodes:\n got %s\nwant %s", got, wantNodes)
	}

	var links []string
	for _, l := range g.Links {
		links = append(links, fmt.Sprintf("%s:%s>%s:%s%v", l.Tail, l.TailPort, l.Head, l.HeadPort, l.Speeds))
	}
	wantLinks := "server:n1:hba0>switch:e1:0[16] switch:c1:16>array:vsp:tgt0[32] switch:c1:17>array:vsp:tgt1[32] switch:e1:46,47>switch:c1:0,1[32 32]"
	if got := strings.Join(links, " "); got != wantLinks {
		t.Errorf("links:\n got %s\nwant %s", got, wantLinks)
	}
}

func TestSanTopologyWithoutWiring(t *testing.T) {
	// An iSCSI initiator: adapters and zoned targets, but no switch knows them.
	g := sanTopology("n1", "node1", []cdb.SANEndpoint{{HBA: "iqn.a", Target: "iqn.t", Array: ""}}, nil)
	if len(g.Nodes) != 1 || g.Nodes[0].Kind != "server" || len(g.Nodes[0].Ports) != 0 || len(g.Links) != 0 {
		t.Errorf("expected the node alone, got %+v", g)
	}
	if g.Links == nil || g.Nodes[0].Ports == nil {
		t.Errorf("links and ports must be empty lists, not null")
	}
}

func TestSanTopologyEntrySwitchKept(t *testing.T) {
	// The adapter is plugged in a switch that reaches no zoned target: still shown.
	ports := []cdb.SwitchPort{{Name: "edge", WWN: "e1", Remote: "hba0", Type: "F-Port", Index: 3, Speed: 8}}
	g := sanTopology("n1", "node1", []cdb.SANEndpoint{{HBA: "hba0", Target: "tgt0", Array: "vsp"}}, ports)
	if len(g.Nodes) != 2 || len(g.Links) != 1 || g.Links[0].HeadPort != "3" {
		t.Errorf("expected the node, its switch and one link, got %+v", g)
	}
}
