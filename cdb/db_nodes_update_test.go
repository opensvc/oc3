package cdb

import (
	"context"
	"testing"
)

// TestUpdateNodeFieldsGoesThroughExecContext checks the update of the fields
// of a node runs through ExecContext, the executor retrying on a deadlock,
// and not on the connection pool directly: a node registering again updates
// its cluster_id while the worker may write the nodes rows of its peer. The
// cluster set, the claim of the node waiting for a member is dropped.
func TestUpdateNodeFieldsGoesThroughExecContext(t *testing.T) {
	rec := &execRecorder{}
	m := testMetrics()
	odb := &DB{DB: rec, HasTx: true, Metrics: m, Session: NewSession(rec, nil)}
	before := counterValue(m.ExecOk)
	if err := odb.UpdateNodeFields(context.Background(), "n1", map[string]any{"cluster_id": "c1"}); err != nil {
		t.Fatal(err)
	}
	if got := counterValue(m.ExecOk) - before; got != 2 {
		t.Errorf("ExecOk grew by %v, want 2: the statements did not go through ExecContext", got)
	}
	if len(rec.queries) != 2 || rec.queries[0] != "UPDATE nodes SET updated = NOW(), cluster_id = ? WHERE node_id = ?" ||
		rec.queries[1] != "DELETE FROM node_cluster_claims WHERE node_id = ?" {
		t.Errorf("queries %q", rec.queries)
	}
}
