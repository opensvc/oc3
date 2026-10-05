package serverhandlers

import (
	"strings"
	"testing"
)

func TestSessionFilter(t *testing.T) {
	node, svc := sessionFilterColumns(propsMapping["node"])
	if node == nil || svc != nil {
		t.Fatalf("node mapping: %v %v", node, svc)
	}
	filters := sessionFilter(node, []string{"n1", "n2"}, nil, nil)
	if len(filters) != 1 || filters[0].Expr != "nodes.node_id IN (?,?)" || len(filters[0].Args) != 2 {
		t.Errorf("nodes: %+v", filters)
	}
	if filters := sessionFilter(node, nil, nil, nil); filters[0].Expr != "1=0" {
		t.Errorf("a filterset matching no node matches no row: %+v", filters)
	}

	node, svc = sessionFilterColumns(propsMapping["alert"])
	if node == nil || svc == nil {
		t.Fatalf("alert mapping: %v %v", node, svc)
	}
	filters = sessionFilter(node, []string{"n1"}, svc, nil)
	if len(filters) != 2 {
		t.Fatalf("alerts: %+v", filters)
	}
	// A row without a node, or without a service, is judged on the other side.
	if !strings.HasPrefix(filters[0].Expr, "(COALESCE(dashboard.node_id, '') = '' OR dashboard.node_id IN (?))") {
		t.Errorf("alert node side: %q", filters[0].Expr)
	}
	if filters[1].Expr != "(COALESCE(dashboard.svc_id, '') = '' OR 1=0)" {
		t.Errorf("alert service side: %q", filters[1].Expr)
	}

	if node, svc := sessionFilterColumns(propsMapping["clusterList"]); node != nil || svc != nil {
		t.Errorf("a list naming neither is left as it is: %v %v", node, svc)
	}
}
