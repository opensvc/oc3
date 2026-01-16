package cdb

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type (
	Dashboard struct {
		ID       int64
		ObjectID string
		NodeID   string
		Type     string
		Fmt      string
		Dict     string
		Severity int
		Env      string
		Instance string
		Created  time.Time
		Updated  time.Time
	}
)

// DashboardInstanceFrozenUpdate update or remove the "service frozen" alerts for instance
func (oDb *DB) DashboardInstanceFrozenUpdate(ctx context.Context, objectID, nodeID string, objectEnv string, frozen bool) error {
	defer logDuration("dashboardInstanceFrozenUpdate", time.Now())
	const (
		queryThawed = `
			DELETE FROM dashboard
			WHERE 
			    dash_type = 'service frozen' 
			  AND svc_id = ?
			  AND node_id = ?
			`
		queryFrozen = `
			INSERT INTO dashboard
			SET
              dash_type = 'service frozen', svc_id = ?, node_id = ?,
              dash_severity = 1, dash_fmt='', dash_dict='',
			  dash_created = NOW(), dash_updated = NOW(), dash_env = ?
		    ON DUPLICATE KEY UPDATE
			  dash_severity = 1, dash_fmt = '', dash_dict = '',
			  dash_updated = NOW(), dash_env = ?`
	)
	var (
		err    error
		result sql.Result
	)
	switch frozen {
	case true:
		result, err = oDb.DB.ExecContext(ctx, queryFrozen, objectID, nodeID, objectEnv, objectEnv)
		if err != nil {
			return fmt.Errorf("update dashboard 'service frozen' for %s@%s: %w", objectID, nodeID, err)
		}
	case false:
		result, err = oDb.DB.ExecContext(ctx, queryThawed, objectID, nodeID)
		if err != nil {
			return fmt.Errorf("delete dashboard 'service frozen' for %s@%s: %w", objectID, nodeID, err)
		}
	}
	if count, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("count dashboard 'service frozen' for %s@%s: %w", objectID, nodeID, err)
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

// dashboardDeleteInstanceNotUpdated delete "instance status not updated" alerts.
func (oDb *DB) DashboardDeleteInstanceNotUpdated(ctx context.Context, objectID, nodeID string) error {
	defer logDuration("dashboardDeleteInstanceNotUpdated", time.Now())
	const (
		query = `DELETE FROM dashboard WHERE svc_id = ? AND node_id = ? AND dash_type = 'instance status not updated'`
	)
	if result, err := oDb.DB.ExecContext(ctx, query, objectID, nodeID); err != nil {
		return err
	} else if count, err := result.RowsAffected(); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

// dashboardDeleteObjectWithType delete from dashboard where svc_id and dash_type match
func (oDb *DB) DashboardDeleteObjectWithType(ctx context.Context, objectID, dashType string) error {
	defer logDuration("dashboardDeleteObjectWithType: "+dashType, time.Now())
	const (
		query = `DELETE FROM dashboard WHERE svc_id = ? AND dash_type = ?`
	)
	if result, err := oDb.DB.ExecContext(ctx, query, objectID, dashType); err != nil {
		return fmt.Errorf("dashboardDeleteObjectWithType %s: %w", dashType, err)
	} else if count, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("dashboardDeleteObjectWithType %s: %w", dashType, err)
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

// ObjectInAckUnavailabilityPeriod returns true if objectID is in acknowledge unavailability period.
func (oDb *DB) ObjectInAckUnavailabilityPeriod(ctx context.Context, objectID string) (ok bool, err error) {
	defer logDuration("ObjectInAckUnavailabilityPeriod", time.Now())
	const (
		query = `SELECT COUNT(*) FROM svcmon_log_ack WHERE svc_id = ? AND mon_begin <= NOW() AND mon_end >= NOW()`
	)
	var count uint64
	err = oDb.DB.QueryRowContext(ctx, query, objectID).Scan(&count)
	if err != nil {
		err = fmt.Errorf("ObjectInAckUnavailabilityPeriod: %w", err)
	}
	return count > 0, err
}

// dashboardUpdateObject delete "service unavailable" alerts.
func (oDb *DB) DashboardUpdateObject(ctx context.Context, d *Dashboard) error {
	defer logDuration("dashboardUpdateObject", time.Now())
	const (
		query = `INSERT INTO dashboard
        	SET
				svc_id = ?,
				dash_type = ?,
				dash_fmt = ?,
				dash_severity = ?,
				dash_dict = ?,
				dash_created = NOW(),
				dash_updated = NOW(),
				dash_env = ?
			ON DUPLICATE KEY UPDATE
				dash_fmt = ?,
				dash_severity = ?,
				dash_dict = ?,
				dash_updated = NOW(),
				dash_env = ?
				`
	)
	result, err := oDb.DB.ExecContext(ctx, query,
		d.ObjectID, d.Type, d.Fmt, d.Severity, d.Dict, d.Env,
		d.Fmt, d.Severity, d.Dict, d.Env)
	if err != nil {
		return fmt.Errorf("dashboardUpdateObject: %w", err)
	} else if count, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("dashboardUpdateObject: %w", err)
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardDeleteNetworkWrongMaskNotUpdated(ctx context.Context) error {
	defer logDuration("DashboardDeleteNetworkWrongMaskNotUpdated", time.Now())
	const (
		query = `DELETE FROM dashboard
                  WHERE
                    dash_type="netmask misconfigured" AND
                    dash_updated < DATE_SUB(NOW(), INTERVAL 1 MINUTE)`
	)
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("DashboardDeleteNetworkWrongMaskNotUpdated: %w", err)
	} else if count, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("DashboardDeleteNetworkWrongMaskNotUpdated: %w", err)
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardDeleteActionErrors(ctx context.Context) error {
	defer logDuration("DashboardDeleteActionErrors", time.Now())
	const (
		query = `DELETE FROM dashboard
                  WHERE
                    dash_type LIKE "%action err%" AND
                    (svc_id, node_id) NOT IN (
                      SELECT svc_id, node_id
                      FROM b_action_errors
                    )`
	)
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("DashboardDeleteActionErrors: %w", err)
	} else if count, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("DashboardDeleteActionErrors: %w", err)
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) PurgeAlertsOnDeletedNodes(ctx context.Context) error {
	const (
		query = `DELETE d
			FROM dashboard d
			LEFT JOIN nodes n ON d.node_id = n.node_id
			WHERE
			  n.node_id IS NULL AND
			  d.node_id != ""`
	)
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if count, err := result.RowsAffected(); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) PurgeAlertsOnDeletedInstances(ctx context.Context) error {
	const (
		query = `DELETE d
			FROM dashboard d
			LEFT JOIN svcmon n ON d.node_id = n.node_id AND d.svc_id = n.svc_id
			WHERE
			  n.node_id IS NULL AND
			  n.svc_id IS NULL AND
			  d.node_id != "" AND
			  d.svc_id != ""`
	)
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if count, err := result.RowsAffected(); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) PurgeAlertsOnDeletedServices(ctx context.Context) error {
	const (
		query = `DELETE d
			FROM dashboard d
			LEFT JOIN services n ON d.svc_id = n.svc_id
			WHERE
			  n.svc_id IS NULL AND
			  d.svc_id != ""`
	)
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if count, err := result.RowsAffected(); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardUpdateNodesNotUpdated(ctx context.Context) error {
	request := `INSERT INTO dashboard
               SELECT
                 NULL,
                 "node information not updated",
                 "",
                 0,
                 "",
                 "",
                 updated,
                 "",
                 node_env,
                 NOW(),
                 node_id,
                 NULL,
                 NULL
               FROM nodes
               WHERE updated < date_sub(NOW(), interval 25 hour)
               ON DUPLICATE KEY UPDATE
                 dash_updated=NOW()`
	result, err := oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardUpdateChecksNotUpdated(ctx context.Context) error {
	request := `
		DELETE FROM dashboard
		WHERE
		  dash_type = "check value not updated" AND
		  node_id NOT IN (SELECT DISTINCT node_id FROM checks_live)
	`
	result, err := oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}

	request = `
		DELETE FROM dashboard
		WHERE id IN (
		    -- Suppression des "check out of bounds" non correspondants
		    SELECT d.id FROM dashboard d
		    LEFT JOIN checks_live c ON
			d.dash_dict_md5 = MD5(CONCAT(
			    '{"ctype": "', c.chk_type,
			    '", "inst": "', c.chk_instance,
			    '", "ttype": "', c.chk_threshold_provider,
			    '", "val": ', c.chk_value,
			    ', "min": ', c.chk_low,
			    ', "max": ', c.chk_high, '}'))
			AND d.node_id = c.node_id
		    WHERE
			d.dash_type = "check out of bounds"
			AND c.id IS NULL

		    UNION ALL

		    -- Suppression des "check value not updated" non correspondants
		    SELECT d.id FROM dashboard d
		    LEFT JOIN checks_live c ON
			d.dash_dict_md5 = MD5(CONCAT('{"i":"', c.chk_instance, '", "t":"', c.chk_type, '"}'))
			AND d.node_id = c.node_id
		    WHERE
			d.dash_type = "check value not updated"
			AND c.id IS NULL
		)
	`
	result, err = oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}
	request = `
	INSERT INTO dashboard
	SELECT
	    NULL,
	    "check value not updated",
	    "",
	    IF(n.node_env = "PRD", 1, 0),
	    "%(t)s:%(i)s",
	    CONCAT('{"i":"', chk_instance, '", "t":"', chk_type, '"}'),
	    chk_updated,
	    MD5(CONCAT('{"i":"', chk_instance, '", "t":"', chk_type, '"}')),
	    n.node_env,
	    NOW(),
	    c.node_id,
	    NULL,
	    CONCAT(chk_type, ":", chk_instance)
	FROM checks_live c
	JOIN nodes n ON c.node_id = n.node_id
	WHERE chk_updated < DATE_SUB(NOW(), INTERVAL 1 DAY)
	ON DUPLICATE KEY UPDATE dash_updated = NOW();
	`
	result, err = oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) AlertActionErrors(ctx context.Context, line BActionErrorCount) error {
	var (
		env      string
		severity int
	)
	if line.SvcEnv != nil && *line.SvcEnv == "PRD" {
		env = *line.SvcEnv
		severity = 4
	} else {
		env = "TST"
		severity = 3
	}
	request := `
                 INSERT INTO dashboard
                 SET
                   dash_type="action errors",
                   svc_id=?,
                   node_id=?,
                   dash_severity=?,
                   dash_fmt="%(err)s action errors",
                   dash_dict=CONCAT('{"err": "', ?, '"}'),
                   dash_created=NOW(),
                   dash_env=?,
                   dash_updated=NOW()
                 ON DUPLICATE KEY UPDATE
                   dash_severity=?,
                   dash_fmt="%(err)s action errors",
                   dash_dict=CONCAT('{"err": "', ?, '"}'),
                   dash_updated=NOW()`
	result, err := oDb.DB.ExecContext(ctx, request, line.SvcID, line.NodeID, severity, line.ErrCount, env, severity, line.ErrCount)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardDeleteActionErrorsWithNoError(ctx context.Context) error {
	request := `
             DELETE FROM dashboard
             WHERE
               dash_dict='{"err": "0"}' and
               dash_type='action errors'
	`
	result, err := oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardUpdateServiceConfigNotUpdated(ctx context.Context) error {
	request := `
	     INSERT INTO dashboard
             SELECT
               NULL,
               "service configuration not updated",
               svc_id,
               IF(svc_env="PRD", 1, 0),
               "",
               "",
               updated,
               "",
               svc_env,
               NOW(),
               "",
               NULL,
               NULL
             FROM services
             WHERE updated < DATE_SUB(NOW(), INTERVAL 25 HOUR)
             ON DUPLICATE KEY UPDATE
               dash_updated=NOW()
	`
	result, err := oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardUpdateInstancesNotUpdated(ctx context.Context) error {
	request := `
		INSERT INTO dashboard
		SELECT
		  NULL,
		  "service status not updated",
		  svc_id,
		  IF(mon_svctype="PRD", 1, 0),
		  "",
		  "",
		  mon_updated,
		  "",
		  mon_svctype,
		  NOW(),
		  node_id,
		  NULL,
		  NULL
		FROM svcmon
		WHERE mon_updated < DATE_SUB(NOW(), INTERVAL 16 MINUTE)
		ON DUPLICATE KEY UPDATE
		  dash_updated=NOW()
	`
	result, err := oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}
	request = `
		DELETE FROM dashboard
		WHERE id IN (
		    SELECT dashboard.id
		    FROM dashboard
		    LEFT JOIN svcmon ON
			dashboard.svc_id = svcmon.svc_id AND
			dashboard.node_id = svcmon.node_id
		    WHERE
			dashboard.dash_type = "service status not updated" AND
			dashboard.svc_id != "" AND
			dashboard.node_id != "" AND
			(svcmon.id IS NULL OR svcmon.mon_updated >= DATE_SUB(NOW(), INTERVAL 16 MINUTE))
		)
	`
	result, err = oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardUpdateNodeMaintenanceExpired(ctx context.Context) error {
	request := `SET @now = NOW()`
	result, err := oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}

	request = `
		INSERT INTO dashboard
		SELECT
		  NULL,
                  "node maintenance expired",
                  "",
                  1,
                  "",
                  "",
                  @now,
                  "",
                  node_env,
                  @now,
                  node_id,
                  NULL,
                  NULL
                 FROM nodes
                 WHERE
                   maintenance_end IS NOT NULL AND
                   maintenance_end != "0000-00-00 00:00:00" AND
                   maintenance_end < @now
		ON DUPLICATE KEY UPDATE
		  dash_updated=@now
	`
	result, err = oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}
	request = `
		DELETE FROM dashboard
		WHERE
		  dash_type="node maintenance expired" AND
		  (
		    dash_updated < @now or
		    dash_updated IS NULL
		  )
	`
	result, err = oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardUpdateNodeCloseToMaintenanceEnd(ctx context.Context) error {
	request := `SET @now = NOW()`
	result, err := oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}

	request = `
		INSERT INTO dashboard
		SELECT
		  NULL,
		  "node close to maintenance end",
		  "",
		  0,
		  "",
		  "",
		  @now,
		  "",
		  node_env,
		  @now,
		  node_id,
		  NULL,
		  NULL
		FROM nodes
		WHERE
		  maintenance_end IS NOT NULL AND
		  maintenance_end != "0000-00-00 00:00:00" AND
		  maintenance_end > DATE_SUB(@now, INTERVAL 30 DAY) AND
		  maintenance_end > @now
		ON DUPLICATE KEY UPDATE
		  dash_updated=@now
	`
	result, err = oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}
	request = `
		DELETE FROM dashboard
		WHERE
		  dash_type="node close to maintenance end" AND
		  (
		    dash_updated < @now or
		    dash_updated IS NULL
		  )
	`
	result, err = oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardUpdateNodeWithoutMaintenanceEnd(ctx context.Context) error {
	request := `SET @now = NOW()`
	result, err := oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}

	request = `
		INSERT INTO dashboard
		SELECT
		  NULL,
		  "node without maintenance end date",
		  "",
		  0,
		  "",
		  "",
		  @now,
		  "",
		  node_env,
		  @now,
		  node_id,
		  NULL,
		  NULL
		FROM nodes
		WHERE
                 (warranty_end IS NULL OR
                  warranty_end = "0000-00-00 00:00:00" OR
                  warranty_end < @now) AND
                 (maintenance_end IS NULL OR
                  maintenance_end = "0000-00-00 00:00:00") AND
                 model not like "%virt%" AND
                 model not like "%Not Specified%" AND
                 model not like "%KVM%"
		ON DUPLICATE KEY UPDATE
		  dash_updated=@now
	`
	result, err = oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}
	request = `
		DELETE FROM dashboard
		WHERE
		  dash_type="node without maintenance end date" AND
		  (
		    dash_updated < @now or
		    dash_updated IS NULL
		  )
	`
	result, err = oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardUpdateAppWithoutResponsible(ctx context.Context) error {
	request := `
		DELETE FROM dashboard
		WHERE
		  dash_type="application code without responsible" and
		  (
		    dash_dict IN (
		      SELECT
		        JSON_OBJECT("a", a.app)
		      FROM apps a JOIN apps_responsibles ar ON a.id=ar.app_id
		    ) OR
		    dash_dict = "" OR
		    dash_dict IS NULL
		  )
	`
	result, err := oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}

	request = `
		DELETE FROM dashboard
		WHERE
                  dash_type="application code without responsible" AND
                  dash_dict NOT IN (
                    SELECT
                      JSON_OBJECT("a", a.app)
                    FROM apps a
                )
	`
	result, err = oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}

	request = `
		INSERT INTO dashboard
		SELECT
		  NULL,
		  "application code without responsible",
		  "",
		  2,
		  "%(a)s",
		  JSON_OBJECT("a", a.app),
		  NOW(),
		  MD5(JSON_OBJECT("a", a.app)),
		  "",
		  NOW(),
		  "",
		  NULL,
		  a.app
		FROM apps a LEFT JOIN apps_responsibles ar ON a.id=ar.app_id
		WHERE
		  ar.group_id IS NULL
		ON DUPLICATE KEY UPDATE
		  dash_updated=NOW()
	`
	result, err = oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}

func (oDb *DB) DashboardUpdatePkgDiff(ctx context.Context) error {
	request := `SET @now = NOW()`
	result, err := oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}

	request = `
		INSERT INTO dashboard (
		    dash_type,
		    svc_id,
		    node_id,
		    dash_severity,
		    dash_fmt,
		    dash_dict,
		    dash_dict_md5,
		    dash_created,
		    dash_updated,
		    dash_env
		)
		WITH
		pkg_cluster_counts AS (
		    SELECT
			nodes.cluster_id,
			nodes.node_id,
			packages.pkg_name,
			packages.pkg_version,
			packages.pkg_arch,
			packages.pkg_type,
			COUNT(DISTINCT nodes.node_id) AS node_count
		    FROM packages
		    JOIN nodes USING (node_id)
		    WHERE packages.pkg_name NOT LIKE 'gpg-pubkey%'
		    GROUP BY
			nodes.cluster_id,
			packages.pkg_name,
			packages.pkg_version,
			packages.pkg_arch,
			packages.pkg_type
		),

		cluster_nodes_counts AS (
		    SELECT
			cluster_id,
			COUNT(node_id) AS node_count,
			GROUP_CONCAT(nodename SEPARATOR ", ") AS nodenames
		    FROM nodes
		    GROUP BY cluster_id
		),

		pkg_cluster_diffs AS (
		    SELECT
			pkg_cluster_counts.*,
			cluster_nodes_counts.nodenames,
			cluster_nodes_counts.node_count AS cluster_node_count
		    FROM pkg_cluster_counts
		    JOIN cluster_nodes_counts
		    USING (cluster_id)
		    WHERE
			pkg_cluster_counts.node_count < cluster_nodes_counts.node_count
		),

		pkg_cluster_diffcount AS (
		    SELECT
			nodes.cluster_id,
			nodes.nodename,
			nodes.node_id,
			pkg_cluster_diffs.nodenames,
			COUNT(*) AS pkg_diffs,
			GROUP_CONCAT(DISTINCT pkg_cluster_diffs.pkg_name SEPARATOR ", ") AS pkg_names
		    FROM pkg_cluster_diffs
		    JOIN nodes USING (node_id)
		    GROUP BY
			nodes.cluster_id
		),

		alerts AS (
		    SELECT
			services.svcname,
			svcmon.svc_id,
			svcmon.mon_svctype,
			pkg_cluster_diffcount.*
		    FROM svcmon 
		    JOIN pkg_cluster_diffcount USING (node_id)
		    JOIN services USING (svc_id)
		)

		SELECT
		    'package differences in cluster' AS dash_type,
		    a.svc_id,
		    NULL AS node_id,
		    IF(a.mon_svctype = "PRD", 1, 0) AS dash_severity,
		    "%(n)d package differences in cluster %(nodes)s" AS dash_fmt,
		    JSON_OBJECT(
			'n', a.pkg_diffs,
			'nodes', a.nodenames,
			'cluster_id', a.cluster_id
		    ) AS dash_dict,
		    MD5(CONCAT(
			a.pkg_diffs,
			a.nodenames,
			a.cluster_id
		    )) AS dash_dict_md5,
		    @now AS dash_created,
		    @now AS dash_updated,
		    a.mon_svctype AS dash_env
		FROM alerts a

		ON DUPLICATE KEY UPDATE
		    dash_severity = VALUES(dash_severity),
		    dash_fmt = VALUES(dash_fmt),
		    dash_dict = VALUES(dash_dict),
		    dash_dict_md5 = VALUES(dash_dict_md5),
		    dash_updated = VALUES(dash_updated),
		    dash_env = VALUES(dash_env)
    	`
	result, err = oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}

	request = `
                DELETE FROM dashboard
                WHERE
                  dash_type="package differences in cluster" AND
                  (
                    dash_updated < @now or
                    dash_updated IS NULL
                  )
        `
	result, err = oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}

	return nil
}

func (oDb *DB) DashboardUpdatePkgDiffForNode(ctx context.Context, nodeID string) error {
	request := `SET @now = NOW()`
	_, err := oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}

	processSvcID := func(svcID, monSvctype, monVmtype string) error {
		var query string
		if monVmtype != "" {
			// encap peers
			query = `
				SELECT DISTINCT nodes.node_id, nodes.nodename
				FROM svcmon
				JOIN nodes ON svcmon.node_id = nodes.node_id
				WHERE svcmon.svc_id = ?
				AND svcmon.mon_updated > DATE_SUB(NOW(), INTERVAL 20 MINUTE)
				AND svcmon.mon_vmtype != ""
				ORDER BY nodes.nodename
			`
		} else {
			// non-encap peers
			query = `
				SELECT DISTINCT nodes.node_id, nodes.nodename
				FROM svcmon
				JOIN nodes ON svcmon.node_id = nodes.node_id
				WHERE svcmon.svc_id = ?
				AND svcmon.mon_updated > DATE_SUB(NOW(), INTERVAL 20 MINUTE)
				AND svcmon.mon_vmtype = ""
				ORDER BY nodes.nodename
			`
		}

		rows, err := oDb.DB.QueryContext(ctx, query, svcID)
		if err != nil {
			return fmt.Errorf("failed to query nodes: %v", err)
		}
		defer rows.Close()

		var nodeIDs []string
		var nodenames []string
		for rows.Next() {
			var nodeID string
			var nodename string
			if err := rows.Scan(&nodeID, &nodename); err != nil {
				return fmt.Errorf("failed to scan node row: %v", err)
			}
			nodeIDs = append(nodeIDs, nodeID)
			nodenames = append(nodenames, nodename)
		}

		if len(nodeIDs) < 2 {
			return nil
		}

		// Count pkg diffs
		var pkgDiffCount int
		placeholders := make([]string, len(nodeIDs))
		args := make([]any, len(nodeIDs))
		for i, id := range nodeIDs {
			placeholders[i] = "?"
			args[i] = id
		}
		query = fmt.Sprintf(`
			SELECT COUNT(pkg_name)
			FROM (
				SELECT
					pkg_name,
					pkg_version,
					pkg_arch,
					pkg_type,
					COUNT(DISTINCT node_id) AS c
				FROM packages
				WHERE
					node_id IN (%s)
					AND pkg_name NOT LIKE "gpg-pubkey%%"
				GROUP BY
					pkg_name,
					pkg_version,
					pkg_arch,
					pkg_type
			) AS t
			WHERE t.c != ?
		`, strings.Join(placeholders, ","))
		args = append(args, len(nodeIDs))

		err = oDb.DB.QueryRowContext(ctx, query, args...).Scan(&pkgDiffCount)
		if err != nil {
			return fmt.Errorf("failed to count package differences: %v", err)
		}

		if pkgDiffCount == 0 {
			return nil
		}

		sev := 0
		if monSvctype == "PRD" {
			sev = 1
		}

		// truncate too long node names list
		skip := 0
		trail := ""
		nodesStr := strings.Join(nodenames, ",")
		for len(nodesStr)+len(trail) > 50 {
			skip++
			nodenames = nodenames[:len(nodenames)-1]
			nodesStr = strings.Join(nodenames, ",")
			trail = fmt.Sprintf(", ... (+%d)", skip)
		}
		nodesStr += trail

		// Format dash_dict JSON content
		dashDict := map[string]any{
			"n":     pkgDiffCount,
			"nodes": nodesStr,
		}
		dashDictJSON, err := json.Marshal(dashDict)
		if err != nil {
			return fmt.Errorf("failed to marshal dash_dict: %v", err)
		}

		dashDictMD5 := fmt.Sprintf("%x", md5.Sum(dashDictJSON))

		query = `
			INSERT INTO dashboard
			SET
				dash_type = "package differences in cluster",
				svc_id = ?,
				node_id = "",
				dash_severity = ?,
				dash_fmt = "%(n)s package differences in cluster %(nodes)s",
				dash_dict = ?,
				dash_dict_md5 = ?,
				dash_created = @now,
				dash_updated = @now,
				dash_env = ?
			ON DUPLICATE KEY UPDATE
				dash_severity = ?,
				dash_fmt = "%(n)s package differences in cluster %(nodes)s",
				dash_dict = ?,
				dash_dict_md5 = ?,
				dash_updated = @now,
				dash_env = ?
		`

		_, err = oDb.DB.ExecContext(ctx, query,
			svcID, sev, dashDictJSON, dashDictMD5, monSvctype,
			sev, dashDictJSON, dashDictMD5, monSvctype,
		)
		if err != nil {
			return fmt.Errorf("failed to insert/update dashboard: %v", err)
		}

		return nil
	}

	// Get the list of svc_id for instances having recently updated status
	rows, err := oDb.DB.QueryContext(ctx, `
		SELECT svcmon.svc_id, svcmon.mon_svctype, svcmon.mon_vmtype
		FROM svcmon
		JOIN nodes ON svcmon.node_id = nodes.node_id
		WHERE svcmon.node_id = ? AND svcmon.mon_updated > DATE_SUB(NOW(), INTERVAL 19 MINUTE)
	`, nodeID)
	if err != nil {
		return fmt.Errorf("failed to query svcmon: %v", err)
	}
	defer rows.Close()

	var svcIDs []any
	todo := make(map[string]func() error)
	for rows.Next() {
		var svcID string
		var monSvctype, monVmtype sql.NullString
		if err := rows.Scan(&svcID, &monSvctype, &monVmtype); err != nil {
			return fmt.Errorf("failed to scan svcmon row: %v", err)
		}

		// Remember which svc_id needs non-updated alert clean up
		svcIDs = append(svcIDs, svcID)

		// Defer after rows.Close() to avoid busy db conn errors
		todo[svcID] = func() error {
			return processSvcID(svcID, monSvctype.String, monVmtype.String)
		}
	}
	rows.Close()
	for svcID, fn := range todo {
		if err := fn(); err != nil {
			return fmt.Errorf("failed to process svc_id %s: %v", svcID, err)
		}
	}

	// Clean up non updated alerts
	if len(svcIDs) > 0 {
		query := fmt.Sprintf(`
			DELETE FROM dashboard
			WHERE svc_id IN (%s)
			AND dash_type = "package differences in cluster"
			AND dash_updated < @now
		`, Placeholders(len(svcIDs)))

		_, err := oDb.DB.ExecContext(ctx, query, svcIDs...)
		if err != nil {
			return fmt.Errorf("failed to delete old dashboard entries: %v", err)
		}
	}

	return nil
}
