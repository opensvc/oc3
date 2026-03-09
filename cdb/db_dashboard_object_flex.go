package cdb

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type (
	DashboardUpdateObjectFlexStartedParams struct {
		SvcID string
		Env   string
		Sev   int
	}
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

	if count, err := oDb.execCountContext(ctx, query, obj.SvcID, sev, obj.Env, obj.SvcID, obj.SvcID); err != nil {
		return fmt.Errorf("dashboardDeleteObjectWithType flex error: %w", err)
	} else if count > 0 {
		defer oDb.SetChange("dashboard")
	} else if count, err := oDb.execCountContext(ctx, queryDelete, obj.SvcID); err != nil {
		return fmt.Errorf("dashboardDeleteObjectWithType clean obsolete flex error: %w", err)
	} else if count > 0 {
		defer oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardUpdateObjectFlexStartedBatch(ctx context.Context, l ...*DashboardUpdateObjectFlexStartedParams) error {
	defer logDuration(fmt.Sprintf("DashboardUpdateObjectFlexStartedBatch %d", len(l)), time.Now())
	if len(l) == 0 {
		return nil
	}

	// 1. Prepare placeholders and arguments for the batch
	// Each param needs 3 values (?, ?, ?)
	var unionParts []string
	var args []interface{}
	var svcIDs []interface{}

	for i, p := range l {
		if i == 0 {
			unionParts = append(unionParts, "SELECT ? AS b_svc_id, ? AS b_sev, ? AS b_env")
		} else {
			unionParts = append(unionParts, "SELECT ?, ?, ?")
		}
		args = append(args, p.SvcID, p.Sev, p.Env)
		svcIDs = append(svcIDs, p.SvcID)
	}

	unionClause := strings.Join(unionParts, " UNION ALL ")

	// 2. The Batched INSERT/UPDATE Query
	// Note: We use VALUES(col) in ON DUPLICATE KEY for cleaner MariaDB 10.4 syntax
	queryInsert := fmt.Sprintf(`
        INSERT INTO dashboard
            (dash_type, dash_fmt, dash_dict, dash_dict_md5, dash_created, dash_updated, 
             svc_id, dash_severity, dash_env)
        SELECT 
            "flex error",
            "%%(n)d instances started. thresholds: %%(smin)d - %%(smax)d",
            CONCAT('{"n": ', t.up, ', "smin": ', t.svc_flex_min_nodes,', "smax": ', t.svc_flex_max_nodes, '}'),
            MD5(CONCAT('{"n": ', t.up, ', "smin": ', t.svc_flex_min_nodes,', "smax": ', t.svc_flex_max_nodes, '}')),
            NOW(), 
            NOW(),
            t.svc_id, 
            t.b_sev, 
            t.b_env
        FROM (
            SELECT 
                p.svc_id, bp.b_sev, bp.b_env, p.svc_flex_min_nodes, p.svc_flex_max_nodes,
                (SELECT COUNT(DISTINCT(c.node_id)) FROM svcmon c WHERE c.svc_id = p.svc_id AND c.mon_availstatus = "up") AS up
            FROM (%s) bp
            JOIN v_svcmon p ON p.svc_id = bp.b_svc_id
            WHERE p.svc_topology = "flex" AND NOT p.svc_availstatus = "n/a"
        ) t
        WHERE (t.svc_flex_min_nodes > 0 AND t.up < t.svc_flex_min_nodes)
           OR (t.svc_flex_max_nodes > 0 AND t.up > t.svc_flex_max_nodes)
        ON DUPLICATE KEY UPDATE 
            dash_updated = NOW(),
            dash_dict = VALUES(dash_dict),
            dash_dict_md5 = VALUES(dash_dict_md5)
    `, unionClause)

	// Execute the batch insert
	_, err := oDb.ExecContext(ctx, queryInsert, args...)
	if err != nil {
		return fmt.Errorf("batch flex error insert: %w", err)
	}

	// 3. Clean up obsolete entries for IDs that NO LONGER violate thresholds
	// This uses an "IN" clause for the batch of IDs
	queryDelete := fmt.Sprintf(`
        DELETE d FROM dashboard d
        LEFT JOIN (
            -- Re-calculate which ones SHOULD have an error
            SELECT p.svc_id
            FROM v_svcmon p
            WHERE p.svc_id IN (%s)
              AND p.svc_topology = "flex"
              AND ((p.svc_flex_min_nodes > 0 AND (SELECT COUNT(DISTINCT node_id) FROM svcmon WHERE svc_id = p.svc_id AND mon_availstatus = "up") < p.svc_flex_min_nodes)
               OR (p.svc_flex_max_nodes > 0 AND (SELECT COUNT(DISTINCT node_id) FROM svcmon WHERE svc_id = p.svc_id AND mon_availstatus = "up") > p.svc_flex_max_nodes))
        ) as still_errors ON d.svc_id = still_errors.svc_id
        WHERE d.svc_id IN (%s)
          AND d.dash_type = "flex error"
          AND d.dash_fmt LIKE "%%instances started%%"
          AND still_errors.svc_id IS NULL`,
		placeholders(len(svcIDs)), placeholders(len(svcIDs)))

	// Combine args for the two IN clauses
	deleteArgs := append(svcIDs, svcIDs...)
	if _, err := oDb.ExecContext(ctx, queryDelete, deleteArgs...); err != nil {
		return fmt.Errorf("batch clean obsolete flex errors: %w", err)
	}

	return nil
}

// Helper to generate (?, ?, ?) strings
func placeholders(n int) string {
	ps := make([]string, n)
	for i := range ps {
		ps[i] = "?"
	}
	return strings.Join(ps, ",")
}
