package serverhandlers

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/server"
)

type (
	fakeEnrollNode struct {
		nodeID          string
		nodename        string
		app             string
		teamResponsible string
		clusterID       string
	}

	// fakeEnrollStore holds the nodes and the auth_node uuids by node id.
	fakeEnrollStore struct {
		nodes []*fakeEnrollNode
		uuids map[string]string
	}
)

func (s *fakeEnrollStore) NodeIDsByNodenameAndAuthUUID(_ context.Context, nodename, nodeUUID string) ([]string, error) {
	var l []string
	for nodeID, u := range s.uuids {
		if n := s.node(nodeID); u == nodeUUID && n != nil && n.nodename == nodename {
			l = append(l, nodeID)
		}
	}
	return l, nil
}

func (s *fakeEnrollStore) DetachNodenameFromCluster(_ context.Context, nodename, clusterID, keepNodeID string) error {
	for _, n := range s.nodes {
		if n.nodename == nodename && n.clusterID == clusterID && n.nodeID != keepNodeID {
			n.clusterID = ""
		}
	}
	return nil
}

func (s *fakeEnrollStore) UpdateNodeFields(_ context.Context, nodeID string, fields map[string]any) error {
	n := s.node(nodeID)
	if n == nil {
		return errors.New("no such node")
	}
	n.app = fields["app"].(string)
	n.teamResponsible = fields["team_responsible"].(string)
	n.clusterID = fields["cluster_id"].(string)
	return nil
}

func (s *fakeEnrollStore) SetAuthNodeUUID(_ context.Context, nodeID, _, nodeUUID string) error {
	s.uuids[nodeID] = nodeUUID
	return nil
}

func (s *fakeEnrollStore) node(nodeID string) *fakeEnrollNode {
	for _, n := range s.nodes {
		if n.nodeID == nodeID {
			return n
		}
	}
	return nil
}

// newFakeEnrollStore returns a store with:
//
//	n3-old: n3, registered with uuid-old in app dev, previous cluster c-old
//	n3-stale: n3, a former member of the enroller cluster c-new
//	n4: n4, never registered by a uuid, in cluster c-old
func newFakeEnrollStore() *fakeEnrollStore {
	return &fakeEnrollStore{
		nodes: []*fakeEnrollNode{
			{nodeID: "n3-old", nodename: "n3", app: "dev", teamResponsible: "devteam", clusterID: "c-old"},
			{nodeID: "n3-stale", nodename: "n3", app: "prod", teamResponsible: "ops", clusterID: "c-new"},
			{nodeID: "n4", nodename: "n4", app: "dev", clusterID: "c-old"},
		},
		uuids: map[string]string{"n3-old": "uuid-old"},
	}
}

func newEnroll(nodename, prevUUID string) nodeEnroll {
	return nodeEnroll{
		nodename:        nodename,
		prevUUID:        prevUUID,
		app:             "prod",
		teamResponsible: "ops",
		clusterID:       "c-new",
	}
}

func newUUIDFunc() func() string {
	return func() string { return "uuid-new" }
}

// TestEnrollNodeByPreviousUUID pins that the joining node found by its
// nodename and its previous uuid moves to the enroller app, team and cluster
// with a new uuid, and that the stale node of the enroller cluster with its
// nodename leaves the cluster, so the nodename designates the joining node
// there.
func TestEnrollNodeByPreviousUUID(t *testing.T) {
	s := newFakeEnrollStore()
	nodeID, nodeUUID, err := enrollNode(context.Background(), s, newEnroll("n3", "uuid-old"), newUUIDFunc())
	if err != nil {
		t.Fatalf("enrollNode: %s", err)
	}
	if nodeID != "n3-old" || nodeUUID != "uuid-new" {
		t.Fatalf("got node %s uuid %s, want n3-old uuid-new", nodeID, nodeUUID)
	}
	n := s.node("n3-old")
	if n.app != "prod" || n.teamResponsible != "ops" || n.clusterID != "c-new" {
		t.Errorf("joining node got app %s team %s cluster %s, want prod ops c-new", n.app, n.teamResponsible, n.clusterID)
	}
	if got := s.uuids["n3-old"]; got != "uuid-new" {
		t.Errorf("joining node auth uuid %s, want uuid-new", got)
	}
	if stale := s.node("n3-stale"); stale.clusterID != "" || stale.app != "prod" {
		t.Errorf("stale node got app %s cluster %q, want it kept in prod with no cluster", stale.app, stale.clusterID)
	}
	if n4 := s.node("n4"); n4.clusterID != "c-old" {
		t.Errorf("node n4 got cluster %s, want c-old unchanged", n4.clusterID)
	}
}

// TestEnrollNodeRefusesNotRegistered pins that an enroller can not register
// a joining node without the uuid it is registered with under its nodename:
// a node that lost its uuid, or never had one, registers itself.
func TestEnrollNodeRefusesNotRegistered(t *testing.T) {
	for name, e := range map[string]nodeEnroll{
		"no uuid":                    newEnroll("n3", ""),
		"no uuid, never registered":  newEnroll("n4", ""),
		"unknown uuid":               newEnroll("n3", "uuid-unknown"),
		"uuid of another nodename":   newEnroll("n4", "uuid-old"),
		"uuid of a deleted node":     newEnroll("n3", "uuid-deleted"),
		"unknown nodename with uuid": newEnroll("n9", "uuid-old"),
	} {
		t.Run(name, func(t *testing.T) {
			s := newFakeEnrollStore()
			s.uuids["deleted"] = "uuid-deleted"
			_, _, err := enrollNode(context.Background(), s, e, newUUIDFunc())
			if !errors.Is(err, errEnrollNotRegistered) {
				t.Fatalf("got error %v, want %v", err, errEnrollNotRegistered)
			}
			if len(s.uuids) != 2 || s.uuids["n3-old"] != "uuid-old" {
				t.Errorf("a refused enroll changed the uuids")
			}
			for _, n := range s.nodes {
				if n.clusterID == "" || n.app == "prod" && n.nodeID != "n3-stale" {
					t.Errorf("a refused enroll changed node %s", n.nodeID)
				}
			}
		})
	}
}

// TestEnrollNodeRefusesAmbiguous pins that a nodename found more than once
// with the previous uuid is refused rather than guessed.
func TestEnrollNodeRefusesAmbiguous(t *testing.T) {
	s := newFakeEnrollStore()
	s.uuids["n3-stale"] = "uuid-old"
	_, _, err := enrollNode(context.Background(), s, newEnroll("n3", "uuid-old"), newUUIDFunc())
	if !errors.Is(err, errEnrollAmbiguous) {
		t.Fatalf("got error %v, want %v", err, errEnrollAmbiguous)
	}
	if !strings.Contains(err.Error(), "2 nodes") {
		t.Errorf("error %q does not count the nodes", err)
	}
	if s.uuids["n3-old"] != "uuid-old" || s.uuids["n3-stale"] != "uuid-old" {
		t.Errorf("a refused enroll changed the uuids")
	}
}

// TestPostAuthNodeEnrollRequiresUUID pins that a joining node registration
// without its previous uuid is a bad request, refused before any lookup.
func TestPostAuthNodeEnrollRequiresUUID(t *testing.T) {
	viper.Set("server.allow_node_enroll", true)
	defer viper.Set("server.allow_node_enroll", nil)
	empty := ""
	for name, prevUUID := range map[string]*string{
		"absent uuid": nil,
		"empty uuid":  &empty,
	} {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c := echo.New().NewContext(httptest.NewRequest(http.MethodPost, "/auth/node", nil), rec)
			body := server.PostAuthNodeJSONBody{Nodename: "n3", Uuid: prevUUID}
			if err := (&Api{}).postAuthNodeEnroll(c, slog.Default(), body); err != nil {
				t.Fatalf("postAuthNodeEnroll: %s", err)
			}
			if rec.Code != http.StatusBadRequest {
				t.Errorf("got status %d, want %d", rec.Code, http.StatusBadRequest)
			}
		})
	}
}
