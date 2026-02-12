package serverhandlers

import (
	"context"
	"net/http"
	"slices"
	"strconv"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/xauth"
)

func (a *Api) PostNodesRegister(c echo.Context) error {
	log := getLog(c)
	odb := a.cdbSession()
	ctx := c.Request().Context()
	odb.CreateTx(ctx, nil)
	ctx, cancel := context.WithTimeout(ctx, a.SyncTimeout)
	defer cancel()

	var success bool

	defer func() {
		if success {
			odb.Commit()
		} else {
			odb.Rollback()
		}
	}()

	var body server.PostNodesRegisterJSONBody

	if err := c.Bind(&body); err != nil {
		log.Error("PostRegister : invalid request body", "error", err)
		return JSONProblemf(c, http.StatusBadRequest, "BadRequest", "invalid request body")
	}

	var app string
	var userID int64

	if body.App != nil {
		app = *body.App
	}

	if !IsAuthByNode(c) {
		// User auth
		user := UserInfoFromContext(c)
		if user == nil {
			return JSONProblemf(c, http.StatusUnauthorized, "Unauthorized", "missing user context")
		}
		userIDStr := user.GetExtensions().Get(xauth.XUserID)
		var err error
		userID, err = strconv.ParseInt(userIDStr, 10, 64)
		if err != nil {
			return JSONProblemf(c, http.StatusBadRequest, "BadRequest", "invalid user id")
		}

		// if the app is not provided, get the default app for the user
		if app == "" {
			defaultApp, err := odb.UserDefaultApp(ctx, &userID)
			if err != nil {
				log.Error("PostRegister: failed to find default app", "error", err)
				return JSONProblemf(c, http.StatusInternalServerError, "InternalError", "cannot find default app")
			}
			app = defaultApp
		} else {
			groups := UserGroupsFromContext(c)
			userApps, err := odb.AppsForGroups(ctx, groups)
			if err != nil {
				log.Error("PostRegister: failed to find user apps", "error", err)
				return JSONProblemf(c, http.StatusInternalServerError, "InternalError", "cannot find user apps")
			}
			if !slices.Contains(userApps, app) {
				return JSONProblemf(c, http.StatusForbidden, "Forbidden", "you are not responsible for the '%s' app", app)
			}
		}
	} else {
		// Node auth
		// Check in config that refuse_anon_register is not set to true
		allowAnonRegister := viper.GetBool("server.allow_anon_register")
		if !allowAnonRegister {
			return JSONProblemf(c, http.StatusForbidden, "Forbidden", "anonymous node registration is disabled")
		}
	}

	var (
		nodeID   string
		nodename = body.Nodename
	)

	node, err := odb.NodeByNodenameAndApp(ctx, nodename, app)
	if err != nil {
		log.Error("PostRegister: failed to find node", "error", err)
		return JSONProblemf(c, http.StatusInternalServerError, "InternalError", "node lookup failed")
	}
	if node != nil {
		// Node already exists in node table
		nodeID = node.NodeID
	} else {
		// Node does not exist: create it with a new node_id
		teamResponsible, _, err := odb.UserDefaultGroup(ctx, userID)
		if err != nil {
			log.Error("PostRegister: failed to find default group", "error", err)
			return JSONProblemf(c, http.StatusInternalServerError, "InternalError", "cannot find default group")
		}

		nodeID = uuid.New().String()
		if err := odb.InsertNode(ctx, nodename, teamResponsible, app, nodeID); err != nil {
			log.Error("PostRegister: failed to insert node", "error", err)
			return JSONProblemf(c, http.StatusInternalServerError, "InternalError", "cannot create node")
		}
	}

	// check if this node_id is already registered
	authNodes, err := odb.AuthNodesByNodeID(ctx, nodeID)
	if err != nil {
		log.Error("PostRegister: failed to find auth_node", "error", err)
		return JSONProblemf(c, http.StatusInternalServerError, "InternalError", "auth_node lookup failed")
	}

	switch len(authNodes) {
	case 1:
		// Already registered: resend uuid
		log.Info("node is already registered", "node_id", nodeID)
		success = true
		return c.JSON(http.StatusOK, map[string]any{
			"uuid": authNodes[0].UUID,
			"info": "node is already registered",
		})
	case 0:
		// New registration: generate uuid, insert auth_node
		u := uuid.New().String()
		if err := odb.InsertAuthNode(ctx, nodename, u, nodeID); err != nil {
			log.Error("PostRegister: failed to insert auth_node", "error", err)
			return JSONProblemf(c, http.StatusInternalServerError, "InternalError", "cannot register node")
		}
		log.Info("node is already registered", "node_id", nodeID)
		success = true
		return c.JSON(http.StatusOK, map[string]any{
			"uuid": u,
			"info": "node is already registered",
		})
	default:
		// Multiple registrations: bug?
		log.Warn("node double registration attempt", "node_id", nodeID, "nodename", nodename)
		success = true
		return c.JSON(http.StatusOK, map[string]any{
			"info": "already registered",
		})
	}
}
