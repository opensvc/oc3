package worker

import (
	"context"
	"database/sql"
	"fmt"
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
