package worker

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (oDb *opensvcDB) findClusterObjectsWithObjectNames(ctx context.Context, clusterID string, objectNames []string) (dbObjects []*DBObject, err error) {
	defer logDuration("findClusterObjectsWithObjectNames", time.Now())
	var query = `
		SELECT svcname, svc_id, cluster_id, svc_availstatus, svc_env, svc_status,
       		svc_placement, svc_provisioned, svc_app
		FROM services
		WHERE cluster_id = ? AND svcname IN (?`
	if len(objectNames) == 0 {
		err = fmt.Errorf("findClusterObjectsWithObjectNames called with empty object name list")
		return
	}
	args := []any{clusterID, objectNames[0]}
	for i := 1; i < len(objectNames); i++ {
		query += ", ?"
		args = append(args, objectNames[i])
	}
	query += ")"

	var rows *sql.Rows
	rows, err = oDb.db.QueryContext(ctx, query, args...)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var o DBObject
		var placement, provisioned, app sql.NullString
		if err = rows.Scan(&o.svcname, &o.svcID, &o.clusterID, &o.availStatus, &o.env, &o.overallStatus, &placement, &provisioned, &app); err != nil {
			return
		}
		o.placement = placement.String
		o.provisioned = provisioned.String
		o.app = app.String
		dbObjects = append(dbObjects, &o)
	}
	err = rows.Err()
	return
}
