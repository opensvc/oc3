package xauth

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"

	"github.com/shaj13/go-guardian/v2/auth"
	"github.com/shaj13/go-guardian/v2/auth/strategies/basic"
)

type (
	authNode struct {
		id        string
		app       string
		clusterID string
		nodeName  string
	}
)

const (
	XNodeID    string = "node_id"
	XNodename  string = "nodename"
	XClusterID string = "cluster_id"
	XApp       string = "app"
)

const (
	queryAuthNode = `SELECT nodes.node_id, nodes.app, nodes.cluster_id, auth_node.nodename
		FROM auth_node 
		JOIN nodes ON nodes.node_id = auth_node.node_id 
		WHERE auth_node.nodename = ? and auth_node.uuid = ?`
)

func NewBasicNode(db *sql.DB) auth.Strategy {
	authFunc := func(ctx context.Context, r *http.Request, userName, password string) (auth.Info, error) {
		u, err := authenticateNode(ctx, db, userName, password)
		if err != nil {
			return nil, fmt.Errorf("invalid credentials")
		}
		return auth.NewUserInfo(userName, u.id, u.Groups(), u.extensions()), nil
	}
	return basic.New(authFunc)
}

func authenticateNode(ctx context.Context, db *sql.DB, nodename, password string) (*authNode, error) {
	var node authNode
	err := db.
		QueryRowContext(ctx, queryAuthNode, nodename, password).
		Scan(&node.id, &node.app, &node.clusterID, &node.nodeName)
	if err != nil {
		return nil, fmt.Errorf("invalid Credentials for node %s", nodename)
	}
	return &node, nil
}

func (n *authNode) extensions() auth.Extensions {
	ext := make(auth.Extensions)
	ext.Set(XNodeID, n.id)
	ext.Set(XApp, n.app)
	ext.Set(XClusterID, n.clusterID)
	ext.Set(XNodename, n.nodeName)
	return ext
}

func (n *authNode) Groups() []string {
	return []string{}
}
