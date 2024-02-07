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
		id  string
		app string
	}
)

const (
	XNodeID string = "node_id"
	XApp    string = "app"
)

const (
	queryAuthNode = "SELECT nodes.node_id, nodes.app " +
		"FROM auth_node " +
		"JOIN nodes ON nodes.node_id = auth_node.node_id " +
		"WHERE auth_node.nodename = ? and auth_node.uuid = ?"
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
		Scan(&node.id, &node.app)
	if err != nil {
		return nil, fmt.Errorf("invalid Credentials for node %s", nodename)
	}
	return &node, nil
}

func (n *authNode) extensions() auth.Extensions {
	ext := make(auth.Extensions)
	ext.Set(XNodeID, n.id)
	ext.Set(XApp, n.app)
	return ext
}

func (n *authNode) Groups() []string {
	return []string{}
}
