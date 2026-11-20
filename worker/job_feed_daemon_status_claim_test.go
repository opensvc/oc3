package worker

import (
	"testing"

	"github.com/opensvc/oc3/cdb"
)

// TestDaemonStatusIgnoredUntilConfirmed checks the report of a node claiming
// a cluster no member confirmed writes nothing past the claim: it would be
// written for the nodes and the objects of that cluster.
func TestDaemonStatusIgnoredUntilConfirmed(t *testing.T) {
	d := newDaemonStatus("n1")
	ops := d.Operations()
	afterClaim := false
	for _, claim := range []cdb.ClusterClaim{cdb.ClusterClaimPending, cdb.ClusterClaimMember, cdb.ClusterClaimNew, cdb.ClusterClaimOrphan} {
		d.clusterClaim = claim
		afterClaim = false
		for _, op := range ops {
			if !afterClaim {
				if op.condition != nil {
					t.Errorf("%s runs before the claim, it must not be conditional", op.name)
				}
				afterClaim = op.name == "dbCheckClusterIDForNodeID"
				continue
			}
			if op.condition == nil {
				t.Fatalf("%s: runs whatever the claim", op.name)
			}
			if got, want := op.condition(), claim.Confirmed(); got != want {
				t.Errorf("claim %s: %s runs %v, want %v", claim, op.name, got, want)
			}
		}
	}
	if !afterClaim {
		t.Fatal("no dbCheckClusterIDForNodeID operation")
	}
}
