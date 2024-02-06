package auth

import (
	"context"
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
	xNodeID string = "node_id"
	xApp    string = "app"
)

func NewBasicNode() auth.Strategy {
	authFunc := func(ctx context.Context, r *http.Request, userName, password string) (auth.Info, error) {
		u, err := authenticateNode(ctx, userName, password)
		if err != nil {
			return nil, fmt.Errorf("invalid credentials")
		}
		return auth.NewUserInfo(userName, u.id, u.Groups(), u.extensions()), nil
	}
	return basic.New(authFunc)
}

func authenticateNode(ctx context.Context, nodename, password string) (*authNode, error) {
	if nodename == "foo" && password == "baar" {
		return &authNode{
			id:  "foo",
			app: "fooApp",
		}, nil
	}
	return nil, fmt.Errorf("invalid Credentials for node %s", nodename)
}

func (n *authNode) extensions() auth.Extensions {
	ext := make(auth.Extensions)
	ext.Set(xNodeID, n.id)
	ext.Set(xApp, n.app)
	return ext
}

func (n *authNode) Groups() []string {
	return []string{}
}
