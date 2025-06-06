package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type (
	DBApp struct {
		id  int64
		app string
	}
)

// authGroupIdsForNode is the oc3 implementation of oc2 node_responsibles(node_id):
//
//	q = db.nodes.node_id == node_id
//	q &= db.apps.app == db.nodes.app
//	q &= db.apps_responsibles.app_id == db.apps.id
//	q &= db.auth_group.id == db.apps_responsibles.group_id
//	rows = db(q).select(db.auth_group.id)
//	return [r.id for r in rows]
func (oDb *opensvcDB) authGroupIdsForNode(ctx context.Context, nodeID string) (groupIds []int64, err error) {
	const query = `
		SELECT auth_group.id FROM auth_group
		JOIN apps_responsibles ON auth_group.id = apps_responsibles.group_id
		JOIN apps ON apps_responsibles.app_id = apps.id
		JOIN nodes ON apps.app = nodes.app
		WHERE nodes.node_id = ?`
	rows, err := oDb.db.QueryContext(ctx, query, nodeID)
	if err != nil {
		err = fmt.Errorf("authGroupIdsForNode: %w", err)
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var groupID sql.NullInt64
		err = rows.Scan(&groupID)
		if err != nil {
			err = fmt.Errorf("authGroupIdsForNode: %w", err)
			return
		}
		if groupID.Valid {
			groupIds = append(groupIds, groupID.Int64)
		}
	}
	if err = rows.Err(); err != nil {
		return
	}
	return
}

// responsibleAppsForNode is the oc3 implementation of oc2 node_responsibles_apps(node_id):
//
//	groups = node_responsibles(node_id)
//	q = db.apps_responsibles.group_id.belongs(groups)
//	q &= db.apps_responsibles.app_id == db.apps.id
//	rows = db(q).select(db.apps.app)
//	return [r.app for r in rows]
func (oDb *opensvcDB) responsibleAppsForNode(ctx context.Context, nodeID string) (apps []string, err error) {
	var query = `
		SELECT apps.app FROM apps
		JOIN apps_responsibles ON apps.id = apps_responsibles.app_id
		JOIN auth_group ON apps_responsibles.group_id = auth_group.id
		WHERE auth_group.id in (
			SELECT auth_group.id FROM auth_group
			JOIN apps_responsibles ON auth_group.id = apps_responsibles.group_id
			JOIN apps ON apps_responsibles.app_id = apps.id
			JOIN nodes ON apps.app = nodes.app
			WHERE nodes.node_id = ?)`
	rows, err := oDb.db.QueryContext(ctx, query, nodeID)
	if err != nil {
		err = fmt.Errorf("responsibleAppsForNode: %w", err)
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var app sql.NullString
		err = rows.Scan(&app)
		if err != nil {
			err = fmt.Errorf("responsibleAppsForNode: %w", err)
			return
		}
		if app.Valid {
			apps = append(apps, app.String)
		}
	}
	if err = rows.Err(); err != nil {
		err = fmt.Errorf("responsibleAppsForNode: %w", err)
		return
	}
	return
}

func (oDb *opensvcDB) appFromAppName(ctx context.Context, app string) (bool, *DBApp, error) {
	const query = "SELECT id, app FROM apps WHERE app = ?"
	var (
		foundID  int64
		foundApp string
	)
	if app == "" {
		return false, nil, fmt.Errorf("can't find app from empty value")
	}
	err := oDb.db.QueryRowContext(ctx, query, app).Scan(&foundID, &foundApp)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil, nil
	case err != nil:
		return false, nil, err
	default:
		return true, &DBApp{id: foundID, app: foundApp}, nil
	}
}

func (oDb *opensvcDB) isAppAllowedForNodeID(ctx context.Context, nodeID, app string) (bool, error) {
	// TODO: apps_responsibles PRIMARY KEY (app_id, group_id)
	const query = "" +
		"SELECT count(*) FROM (" +
		"  SELECT COUNT(`t`.`group_id`) AS `c`" +
		"  FROM (" +
		"      SELECT `ar`.`group_id` FROM `nodes` `n`, `apps` `a`, `apps_responsibles` `ar`" +
		"      WHERE `n`.`app` = `a`.`app` AND `ar`.`app_id` = `a`.`id` AND `n`.`node_id` = ?" +
		"    UNION ALL" +
		"      SELECT `ar`.`group_id` FROM `apps` `a`, `apps_responsibles` `ar`" +
		"      WHERE `ar`.`app_id` = `a`.`id` AND `a`.`app` = ?" +
		"   ) AS `t` GROUP BY `t`.`group_id`) `u`" +
		"WHERE `u`.`c` = 2"
	var found int64
	err := oDb.db.QueryRowContext(ctx, query, nodeID, app).Scan(&found)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	default:
		return found > 0, nil
	}
}

func (oDb *opensvcDB) appFromNodeAndCandidateApp(ctx context.Context, candidateApp string, node *DBNode) (string, error) {
	app := candidateApp
	if candidateApp == "" {
		app = node.app
	} else if ok, err := oDb.isAppAllowedForNodeID(ctx, node.nodeID, candidateApp); err != nil {
		return "", fmt.Errorf("can't detect if app %s is allowed: %w", candidateApp, err)
	} else if !ok {
		app = node.app
	}
	if ok, _, err := oDb.appFromAppName(ctx, app); err != nil {
		return "", fmt.Errorf("can't verify guessed app %s: %w", app, err)
	} else if !ok {
		return "", fmt.Errorf("can't verify guessed app %s: app not found", app)
	}
	return app, nil
}

// appIDFromNodeID retrieves the application ID associated with a given node ID from the database.
// Returns the app ID, a boolean indicating if the app exists, and an error if applicable.
// If the node ID is empty, it returns an error.
func (oDb *opensvcDB) appIDFromNodeID(ctx context.Context, nodeID string) (int64, bool, error) {
	const query = "SELECT `apps`.`id` FROM `apps` JOIN `nodes` ON `apps`.`app` = `nodes`.`app` WHERE `nodes`.`node_id`  = ? LIMIT 1"
	var (
		appID sql.NullInt64
	)
	if nodeID == "" {
		return appID.Int64, false, fmt.Errorf("can't find app from empty node id value")
	}
	err := oDb.db.QueryRowContext(ctx, query, nodeID).Scan(&appID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return appID.Int64, false, nil
	case err != nil:
		return appID.Int64, false, err
	default:
		return appID.Int64, true, nil
	}
}

// appIDFromNodeID retrieves the application ID associated with a given node ID from the database.
// Returns the app ID, a boolean indicating if the app exists, and an error if applicable.
// If the node ID is empty, it returns an error.
func (oDb *opensvcDB) appIDFromObjectID(ctx context.Context, objectID string) (int64, bool, error) {
	const query = "SELECT `apps`.`id` FROM `apps` JOIN `services` ON `apps`.`app` = `services`.`svc_app` WHERE `services`.`svc_id`  = ? LIMIT 1"
	var (
		appID sql.NullInt64
	)
	if objectID == "" {
		return appID.Int64, false, fmt.Errorf("can't find app from empty object id value")
	}
	err := oDb.db.QueryRowContext(ctx, query, objectID).Scan(&appID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return appID.Int64, false, nil
	case err != nil:
		return appID.Int64, false, err
	default:
		return appID.Int64, true, nil
	}
}

// appIDFromObjectOrNodeIDs retrieves the application ID associated with the given objectID or nodeID in the database.
// It first attempts to find the application ID based on the objectID. If unsuccessful, it falls back to using the nodeID.
// Returns the application ID, a boolean indicating success, and an error if any issue occurs during retrieval.
//
// It is a port from oc2 `get_preferred_app`
//
//	    def get_preferred_app(node_id, svc_id):
//		       if svc_id is None:
//		           q = db.nodes.node_id == node_id
//		           q &= db.apps.app == db.nodes.app
//		           return db(q).select(db.apps.ALL).first()
//		       else:
//		           q = db.services.svc_id == svc_id
//		           q &= db.services.svc_app == db.apps.app
//		           row = db(q).select(db.apps.ALL).first()
//		           if row is None:
//		               q = db.nodes.node_id == node_id
//		               q &= db.apps.app == db.nodes.app
//		               return db(q).select(db.apps.ALL).first()
//		           return row
func (odb *opensvcDB) appIDFromObjectOrNodeIDs(ctx context.Context, nodeID, objectID string) (int64, bool, error) {
	if objectID != "" {
		if found, ok, err := odb.appIDFromObjectID(ctx, objectID); err == nil {
			// abort on error
			return found, ok, err
		} else if ok {
			return found, ok, err
		}
	}
	return odb.appIDFromNodeID(ctx, nodeID)
}
