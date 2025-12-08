package cdb

import (
	"context"
	"fmt"
	"time"
)

func (oDb *DB) DashboardUpdateObjectFlexStarted(ctx context.Context, obj *DBObject, sev int) error {
	defer logDuration("dashboardUpdateObjectFlexStarted", time.Now())
	const query = `
		INSERT INTO dashboard
			(dash_type, dash_fmt, dash_dict, dash_dict_md5, dash_created, dash_updated, 
			 svc_id, dash_severity, dash_env)
		SELECT "flex error",
                "%(n)d instances started. thresholds: %(smin)d - %(smax)d",
		        CONCAT('{"n": ', t.up, ', "smin": ', t.svc_flex_min_nodes,', "smax": ', t.svc_flex_max_nodes, '}'),
		        MD5(CONCAT('{"n": ', t.up, ', "smin": ', t.svc_flex_min_nodes,', "smax": ', t.svc_flex_max_nodes, '}')),
			    NOW(), NOW(),
			    ?, ?, ?
		FROM (
				SELECT *
		    	FROM (
					SELECT
						p.svc_flex_min_nodes AS svc_flex_min_nodes,
						p.svc_flex_max_nodes AS svc_flex_max_nodes,
						(SELECT COUNT(DISTINCT(c.node_id)) FROM svcmon c WHERE c.svc_id = ? AND c.mon_availstatus = "up") AS up
					FROM v_svcmon p
					WHERE
						p.svc_id = ?
						AND p.svc_topology = "flex"
						AND NOT p.svc_availstatus = "n/a"
					) w
				WHERE
					(w.svc_flex_min_nodes > 0 AND w.up < w.svc_flex_min_nodes)
					OR
					(w.svc_flex_max_nodes > 0 AND w.up > w.svc_flex_max_nodes)
		) t
		ON DUPLICATE KEY UPDATE 
			dash_updated = NOW(),
		    dash_dict = CONCAT('{"n": ', t.up, ', "smin": ', t.svc_flex_min_nodes,', "smax": ', t.svc_flex_max_nodes, '}'),
		    dash_dict_md5 = MD5(CONCAT('{"n": ', t.up, ', "smin": ', t.svc_flex_min_nodes,', "smax": ', t.svc_flex_max_nodes, '}'))
	`

	const queryDelete = `
		DELETE FROM dashboard
		WHERE svc_id = ? and dash_type = "flex error" and dash_fmt like "%instances started%"`

	if result, err := oDb.DB.ExecContext(ctx, query, obj.SvcID, sev, obj.Env, obj.SvcID, obj.SvcID); err != nil {
		return fmt.Errorf("dashboardDeleteObjectWithType flex error: %w", err)
	} else if count, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("dashboardDeleteObjectWithType count updated: %w", err)
	} else if count > 0 {
		defer oDb.SetChange("dashboard")
	} else if result, err := oDb.DB.ExecContext(ctx, queryDelete, obj.SvcID); err != nil {
		return fmt.Errorf("dashboardDeleteObjectWithType clean obsolete flex error: %w", err)
	} else if count, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("dashboardDeleteObjectWithType count deleted: %w", err)
	} else if count > 0 {
		defer oDb.SetChange("dashboard")
	}
	return nil
}
