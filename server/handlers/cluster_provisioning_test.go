package serverhandlers

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

type (
	fakeBootstrapToken struct {
		node *cdb.DBBootstrapNode
		used bool
	}

	// fakeBootstrapStore holds the tokens by hash, the declarations by
	// cluster id and the nodenames by node id.
	fakeBootstrapStore struct {
		tokens       map[string]*fakeBootstrapToken
		declarations map[string]*cdb.DBClusterDeclaration
		nodenames    map[string]string
		uuids        map[string]string
		clusterIDs   map[string]string
	}
)

func (s *fakeBootstrapStore) BootstrapTokenNode(_ context.Context, h string) (*cdb.DBBootstrapNode, error) {
	if t, ok := s.tokens[h]; ok && !t.used {
		return t.node, nil
	}
	return nil, nil
}

func (s *fakeBootstrapStore) ClusterDeclaration(_ context.Context, id string) (*cdb.DBClusterDeclaration, error) {
	d, ok := s.declarations[id]
	if !ok {
		return nil, nil
	}
	copied := *d
	if d.SeedNodeID != nil {
		name := s.nodenames[*d.SeedNodeID]
		copied.SeedNodename = &name
	}
	return &copied, nil
}

func (s *fakeBootstrapStore) UseBootstrapToken(_ context.Context, h string) (bool, error) {
	t, ok := s.tokens[h]
	if !ok || t.used {
		return false, nil
	}
	t.used = true
	return true, nil
}

func (s *fakeBootstrapStore) ElectClusterSeed(_ context.Context, clusterID, nodeID string) (string, error) {
	d := s.declarations[clusterID]
	if d.SeedNodeID == nil {
		d.SeedNodeID = &nodeID
	}
	return *d.SeedNodeID, nil
}

func (s *fakeBootstrapStore) SetAuthNodeUUID(_ context.Context, nodeID, _, nodeUUID string) error {
	s.uuids[nodeID] = nodeUUID
	return nil
}

func (s *fakeBootstrapStore) UpdateNodeFields(_ context.Context, nodeID string, fields map[string]any) error {
	s.clusterIDs[nodeID], _ = fields["cluster_id"].(string)
	return nil
}

func newFakeBootstrapStore() *fakeBootstrapStore {
	s := &fakeBootstrapStore{
		tokens: map[string]*fakeBootstrapToken{},
		declarations: map[string]*cdb.DBClusterDeclaration{
			"c1": {ClusterID: "c1", ClusterName: "clu1", App: "APP", Heartbeats: `[{"type":"unicast","port":"10000"}]`},
		},
		nodenames:  map[string]string{},
		uuids:      map[string]string{},
		clusterIDs: map[string]string{},
	}
	for _, n := range []string{"n1", "n2"} {
		s.nodenames["id-"+n] = n
		s.tokens[hashBootstrapToken("tk-"+n)] = &fakeBootstrapToken{
			node: &cdb.DBBootstrapNode{NodeID: "id-" + n, Nodename: n, ClusterID: "c1"},
		}
	}
	return s
}

// TestBootstrapNodeSeedThenJoin checks the first node bootstrapped is the
// seed, given the heartbeats, the next one joins it, each given a uuid and
// the app of the declaration.
func TestBootstrapNodeSeedThenJoin(t *testing.T) {
	s := newFakeBootstrapStore()
	ctx := context.Background()
	newUUID := func() string { return "uuid" }

	r, err := bootstrapNode(ctx, s, "N1", "tk-n1", newUUID)
	if err != nil {
		t.Fatal(err)
	}
	if r.Cluster.Role != server.Seed || r.Cluster.Id != "c1" || r.Cluster.Name != "clu1" || r.Cluster.SeedNodename != nil {
		t.Errorf("n1 cluster %+v, want seed of c1", r.Cluster)
	}
	if len(r.Cluster.Heartbeats) != 1 || r.Cluster.Heartbeats[0].Type != "unicast" || r.Cluster.Heartbeats[0].AdditionalProperties["port"] != "10000" {
		t.Errorf("heartbeats %+v", r.Cluster.Heartbeats)
	}
	if r.Uuid != "uuid" || s.uuids["id-n1"] != "uuid" || r.App != "APP" || s.clusterIDs["id-n1"] != "c1" {
		t.Errorf("n1 %+v, uuids %v, cluster ids %v", r, s.uuids, s.clusterIDs)
	}

	r, err = bootstrapNode(ctx, s, "n2", "tk-n2", newUUID)
	if err != nil {
		t.Fatal(err)
	}
	if r.Cluster.Role != server.Join || r.Cluster.SeedNodename == nil || *r.Cluster.SeedNodename != "n1" {
		t.Errorf("n2 cluster %+v, want join of n1", r.Cluster)
	}
}

// TestBootstrapNodeDenied checks a token is refused, and left unused, for
// another nodename, and refused once used or unknown.
func TestBootstrapNodeDenied(t *testing.T) {
	s := newFakeBootstrapStore()
	ctx := context.Background()
	newUUID := func() string { return "uuid" }

	if _, err := bootstrapNode(ctx, s, "n2", "tk-n1", newUUID); !errors.Is(err, errBootstrapDenied) {
		t.Errorf("token of n1 by n2: %v", err)
	}
	if s.tokens[hashBootstrapToken("tk-n1")].used {
		t.Error("token of n1 used by n2")
	}
	if _, err := bootstrapNode(ctx, s, "n1", "tk-n1", newUUID); err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrapNode(ctx, s, "n1", "tk-n1", newUUID); !errors.Is(err, errBootstrapDenied) {
		t.Errorf("token used twice: %v", err)
	}
	for _, tc := range [][2]string{{"n1", "unknown"}, {"", "tk-n2"}, {"n2", ""}} {
		if _, err := bootstrapNode(ctx, s, tc[0], tc[1], newUUID); !errors.Is(err, errBootstrapDenied) {
			t.Errorf("%q %q: %v", tc[0], tc[1], err)
		}
	}
}

// TestBootstrapNodeUndeclared checks a token of a cluster no longer declared
// is refused, and not used.
func TestBootstrapNodeUndeclared(t *testing.T) {
	s := newFakeBootstrapStore()
	delete(s.declarations, "c1")
	if _, err := bootstrapNode(context.Background(), s, "n1", "tk-n1", func() string { return "u" }); err == nil {
		t.Fatal("no error")
	}
	if s.tokens[hashBootstrapToken("tk-n1")].used {
		t.Error("token used")
	}
}

// fakeClaimStore holds pending enrollments by id, in order, and their tokens.
type fakeClaimStore struct {
	order   []int64
	states  map[int64]string
	owners  map[int64]string
	tokens  map[int64]string
	results map[int64]string
	// stolen are the ids another member claims just before this one.
	stolen map[int64]bool
}

func (s *fakeClaimStore) ExpireClusterEnrollments(context.Context, string) error { return nil }

func (s *fakeClaimStore) NextPendingClusterEnrollment(_ context.Context, _, nodeID string) (int64, error) {
	for _, id := range s.order {
		if s.states[id] == "pending" && s.owners[id] != nodeID {
			return id, nil
		}
	}
	return 0, nil
}

func (s *fakeClaimStore) ClaimClusterEnrollment(_ context.Context, id int64, _, _ string) (bool, error) {
	if s.stolen[id] {
		s.states[id] = "claimed"
		return false, nil
	}
	if s.states[id] != "pending" {
		return false, nil
	}
	s.states[id] = "claimed"
	return true, nil
}

func (s *fakeClaimStore) SetClusterEnrollmentResult(_ context.Context, id int64, _, state, message string) (bool, error) {
	s.states[id] = state
	s.results[id] = message
	return true, nil
}

func (s *fakeClaimStore) ClusterEnrollment(_ context.Context, clusterID string, id int64) (*cdb.DBClusterEnrollment, error) {
	return &cdb.DBClusterEnrollment{ID: id, ClusterID: clusterID, NodeID: s.owners[id], State: s.states[id]}, nil
}

func (s *fakeClaimStore) takeJoinToken(_ context.Context, id int64) (string, error) {
	tk := s.tokens[id]
	delete(s.tokens, id)
	return tk, nil
}

// TestClaimEnrollment checks a member skips its own enrollment, one claimed
// by another member meanwhile, fails one whose token expired, and claims the
// next with its token; nothing when none is left.
func TestClaimEnrollment(t *testing.T) {
	s := &fakeClaimStore{
		order:   []int64{1, 2, 3, 4},
		states:  map[int64]string{1: "pending", 2: "pending", 3: "pending", 4: "pending"},
		owners:  map[int64]string{1: "member", 2: "a", 3: "b", 4: "c"},
		tokens:  map[int64]string{2: "tk2", 4: "tk4"},
		results: map[int64]string{},
		stolen:  map[int64]bool{2: true},
	}
	ctx := context.Background()
	e, token, err := claimEnrollment(ctx, s, "c1", "member", "m")
	if err != nil {
		t.Fatal(err)
	}
	if e == nil || e.ID != 4 || token != "tk4" {
		t.Fatalf("claimed %+v %q, want 4 tk4", e, token)
	}
	if s.states[3] != "failed" || !strings.Contains(s.results[3], "expired") {
		t.Errorf("enrollment 3 %s %q, want failed for its token", s.states[3], s.results[3])
	}
	if s.states[1] != "pending" {
		t.Errorf("own enrollment %s, want pending", s.states[1])
	}
	if e, _, err := claimEnrollment(ctx, s, "c1", "member", "m"); err != nil || e != nil {
		t.Errorf("second claim %+v %v, want none", e, err)
	}
}

// TestCheckHeartbeats checks a heartbeat needs a type and keywords as om3
// names them.
func TestCheckHeartbeats(t *testing.T) {
	ok := []server.ClusterHeartbeat{{Type: "unicast", AdditionalProperties: map[string]string{"port": "10000", "nodes@n1": "n1"}}}
	if err := checkHeartbeats(ok); err != nil {
		t.Errorf("valid: %s", err)
	}
	for _, hb := range []server.ClusterHeartbeat{
		{Type: ""},
		{Type: "unicast", AdditionalProperties: map[string]string{"Port": "1"}},
		{Type: "unicast", AdditionalProperties: map[string]string{"po rt": "1"}},
		{Type: "unicast", AdditionalProperties: map[string]string{"port": strings.Repeat("x", 4097)}},
	} {
		if err := checkHeartbeats([]server.ClusterHeartbeat{hb}); err == nil {
			t.Errorf("%+v: no error", hb)
		}
	}
	s, err := encodeHeartbeats(nil)
	if err != nil || s != "[]" {
		t.Errorf("nil heartbeats %q %v", s, err)
	}
}
