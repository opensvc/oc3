package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

type (
	DBApp struct {
		id  int64
		app string
	}

	DBObjStatus struct {
		availStatus string
		status      string
		placement   string
		frozen      string
		provisioned string
	}

	// opensvcDB implements opensvc db functions
	opensvcDB struct {
		db *sql.DB

		tChanges map[string]struct{}
	}
)

// createNewObject creates missing object in database and returns *DBObject.
//
// uniq svcID is found thew findOrCreateObjectID to ensure uniq svcID on concurrent calls
// for same object.
func (oDb *opensvcDB) createNewObject(ctx context.Context, objectName, clusterID, candidateApp string, node *DBNode) (*DBObject, error) {
	created, svcID, err := oDb.findOrCreateObjectID(ctx, objectName, clusterID)
	if err != nil {
		return nil, fmt.Errorf("can't find or create object: %w", err)
	}
	if created {
		slog.Debug(fmt.Sprintf("will create service %s with new svc_id: %s", objectName, svcID))
		if err := oDb.insertOrUpdateObjectForNodeAndCandidateApp(ctx, objectName, svcID, candidateApp, node); err != nil {
			return nil, err
		}
	}
	if obj, err := oDb.objectFromID(ctx, svcID); err != nil {
		return nil, err
	} else if obj == nil {
		// svc id exists without associated service
		slog.Debug(fmt.Sprintf("will create service %s with existing svc_id: %s", objectName, svcID))
		if err := oDb.insertOrUpdateObjectForNodeAndCandidateApp(ctx, objectName, svcID, candidateApp, node); err != nil {
			return nil, err
		}
		if obj, err := oDb.objectFromID(ctx, svcID); err != nil {
			return nil, err
		} else if obj == nil {
			return nil, fmt.Errorf("createNewObject %s: can't retrieve service with svc_id %s", objectName, svcID)
		} else {
			return obj, nil
		}
	} else {
		return obj, nil
	}
}

// pingObject updates services.svc_status_updated and services_log_last.svc_end
// when services.svc_status_updated timestamp for svc_id id older than 30s.
func (oDb *opensvcDB) pingObject(ctx context.Context, svcID string) (updates bool, err error) {
	const UpdateServicesSvcStatusUpdated = "" +
		"UPDATE `services` SET `svc_status_updated` = NOW()" +
		" WHERE `svc_id`= ? " +
		"   AND (`svc_status_updated` < DATE_SUB(NOW(), INTERVAL 30 SECOND) OR `svc_status_updated` is NULL)"
	const updateSvcLogLastSvc = "" +
		"UPDATE `services_log_last` SET `svc_end` = ? WHERE `svc_id`= ? "
	var (
		now         = time.Now()
		result      sql.Result
		rowAffected int64
	)
	result, err = oDb.db.ExecContext(ctx, UpdateServicesSvcStatusUpdated, svcID)
	if err != nil {
		return
	}
	rowAffected, err = result.RowsAffected()
	if err != nil {
		return
	}
	if rowAffected == 0 {
		return
	}

	oDb.tableChange("services")
	updates = true

	_, err = oDb.db.ExecContext(ctx, updateSvcLogLastSvc, now, svcID)

	return
}

// pingInstance updates svcmon.mon_updated, svcmon_log_last.mon_end,
// resmon.updated and resmon_log_last.res_end
// when svcmon.mon_updated timestamp for svc_id id older than 30s.
func (oDb *opensvcDB) pingInstance(ctx context.Context, svcID, nodeID string) (updates bool, err error) {
	const (
		qHasInstance  = "SELECT count(*) FROM `svcmon` WHERE `svc_id` = ? AND `node_id` = ?"
		qUpdateSvcmon = "" +
			"UPDATE `svcmon` SET `mon_updated` = NOW()" +
			" WHERE `svc_id` = ?" +
			"   AND `node_id` = ?" +
			"   AND (`mon_updated` < DATE_SUB(NOW(), INTERVAL 30 SECOND) OR `mon_updated` is NULL)"

		qUpdateSvcmonLogLast = "" +
			"UPDATE `svcmon_log_last` SET `mon_end` = NOW()" +
			" WHERE `svc_id` = ?" +
			"   AND `node_id` = ?" +
			"   AND (`mon_end` < DATE_SUB(NOW(), INTERVAL 30 SECOND) OR `mon_end` is NULL)"

		qUpdateResmon = "" +
			"UPDATE `resmon` SET `updated` = NOW()" +
			" WHERE `svc_id` = ?" +
			"   AND `node_id` = ?" +
			"   AND (`updated` < DATE_SUB(NOW(), INTERVAL 30 SECOND) OR `updated` is NULL)"

		qUpdateResmonLogLast = "" +
			"UPDATE `resmon_log_last` SET `res_end` = NOW()" +
			" WHERE `svc_id` = ?" +
			"   AND `node_id` = ?" +
			"   AND (`res_end` < DATE_SUB(NOW(), INTERVAL 30 SECOND) OR `res_end` is NULL)"
	)
	var (
		count  int64
		result sql.Result
	)
	err = oDb.db.QueryRowContext(ctx, qHasInstance, svcID, nodeID).Scan(&count)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		err = nil
		return
	case err != nil:
		return
	}

	if result, err = oDb.db.ExecContext(ctx, qUpdateSvcmon, svcID, nodeID); err != nil {
		return
	} else if count, err = result.RowsAffected(); err != nil {
		return
	} else if count == 0 {
		return
	}
	updates = true

	oDb.tableChange("svcmon")
	if _, err = oDb.db.ExecContext(ctx, qUpdateSvcmonLogLast, svcID, nodeID); err != nil {
		return
	}

	if result, err = oDb.db.ExecContext(ctx, qUpdateResmon, svcID, nodeID); err != nil {
		return
	} else if count, err = result.RowsAffected(); err != nil {
		return
	} else if count == 0 {
		return
	}
	oDb.tableChange("resmon")

	if _, err = oDb.db.ExecContext(ctx, qUpdateResmonLogLast, svcID, nodeID); err != nil {
		return
	}

	return
}

func (oDb *opensvcDB) updateObjectStatus(ctx context.Context, svcID string, o *DBObjStatus) error {
	const query = "" +
		"UPDATE `services` SET `svc_availstatus` = ?" +
		" , `svc_status` = ?" +
		" , `svc_placement` = ?" +
		" , `svc_frozen` = ?" +
		" , `svc_provisioned` = ?" +
		" , `svc_status_updated` = NOW()" +
		" WHERE `svc_id`= ? "
	if _, err := oDb.db.ExecContext(ctx, query, o.availStatus, o.status, o.placement, o.frozen, o.provisioned, svcID); err != nil {
		return fmt.Errorf("can't update service status %s: %w", svcID, err)
	}
	oDb.tableChange("services")
	return nil
}

// updateObjectLog handle services_log_last and services_log avail value changes.
//
// services_log_last tracks the current avail value from begin to now.
// services_log tracks avail values changes with begin and end: [(avail, begin, end), ...]
func (oDb *opensvcDB) updateObjectLog(ctx context.Context, svcID string, avail string) error {
	/*
		CREATE TABLE `services_log_last` (
		  `id` int(11) NOT NULL AUTO_INCREMENT,
		  `svc_availstatus` varchar(10) NOT NULL,
		  `svc_begin` datetime NOT NULL,
		  `svc_end` datetime NOT NULL,
		  `svc_id` char(36) CHARACTER SET ascii DEFAULT '',
		  PRIMARY KEY (`id`),
		  UNIQUE KEY `uk` (`svc_id`),
		  KEY `k_svc_id` (`svc_id`)
		) ENGINE=InnoDB AUTO_INCREMENT=7778 DEFAULT CHARSET=utf8

		CREATE TABLE `services_log` (
		  `id` int(11) NOT NULL AUTO_INCREMENT,
		  `svc_availstatus` varchar(10) NOT NULL,
		  `svc_begin` datetime NOT NULL,
		  `svc_end` datetime NOT NULL,
		  `svc_id` char(36) CHARACTER SET ascii DEFAULT '',
		  PRIMARY KEY (`id`),
		  KEY `k_svc_id` (`svc_id`),
		  KEY `idx_svc_end` (`svc_end`)
		) ENGINE=InnoDB AUTO_INCREMENT=665687 DEFAULT CHARSET=utf8
	*/
	const (
		qGetLogLast = "SELECT `svc_availstatus`, `svc_begin`, `svc_end` FROM `services_log_last` WHERE `svc_id` = ?"
		qSetLogLast = "" +
			"INSERT INTO `services_log_last` (`svc_id`, `svc_begin`, `svc_end`, `svc_availstatus`)" +
			" VALUES (?, NOW(), NOW(), ?)" +
			" ON DUPLICATE KEY UPDATE `svc_begin` = NOW(), `svc_end` = NOW(), `svc_availstatus` = ?"
		qExtendIntervalOfCurrentAvail                = "UPDATE `services_log_last` SET `svc_end` = NOW() WHERE `svc_id` = ?"
		qSaveIntervalOfPreviousAvailBeforeTransition = "" +
			"INSERT INTO `services_log` (`svc_id`, `svc_begin`, `svc_end`, `svc_availstatus`)" +
			" VALUES (?, ?, NOW(), ?)"
	)
	var (
		previousAvail string

		previousBegin, previousEnd time.Time
	)
	setLogLast := func() error {
		_, err := oDb.db.ExecContext(ctx, qSetLogLast, svcID, avail, avail)
		if err != nil {
			return fmt.Errorf("updateObjectLog can't update services_log_last %s: %w", svcID, err)
		}
		return nil
	}
	err := oDb.db.QueryRowContext(ctx, qGetLogLast, svcID).Scan(&previousAvail, &previousBegin, &previousEnd)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// set initial avail value
		defer oDb.tableChange("service_log")
		return setLogLast()
	case err != nil:
		return fmt.Errorf("updateObjectLog can't get services_log_last %s: %w", svcID, err)
	default:
		defer oDb.tableChange("service_log")
		if previousAvail == avail {
			// no change, extend last interval
			if _, err := oDb.db.ExecContext(ctx, qExtendIntervalOfCurrentAvail, svcID); err != nil {
				return fmt.Errorf("updateObjectLog can't set services_log_last.svc_end %s: %w", svcID, err)
			}
			return nil
		} else {
			// the avail value will change, save interval of previous avail value before change
			if _, err := oDb.db.ExecContext(ctx, qSaveIntervalOfPreviousAvailBeforeTransition, svcID, previousBegin, previousAvail); err != nil {
				return fmt.Errorf("updateObjectLog can't save services_log change %s: %w", svcID, err)
			}
			// reset begin and end interval for new avail
			return setLogLast()
		}
	}
}

// insertOrUpdateObjectForNodeAndCandidateApp will insert or update object with svcID.
//
// If candidate app is not valid, node app will be used (see getAppFromNodeAndCandidateApp)
func (oDb *opensvcDB) insertOrUpdateObjectForNodeAndCandidateApp(ctx context.Context, objectName string, svcID, candidateApp string, node *DBNode) error {
	const query = "" +
		"INSERT INTO `services` (`svcname`, `cluster_id`, `svc_id`, `svc_app`, `svc_env`, `updated`)" +
		" VALUES (?, ?, ?, ?, ?, NOW())" +
		" ON DUPLICATE KEY UPDATE" +
		"    `svcname` = ?, `cluster_id` = ?, `svc_app` = ?, `svc_env` = ?, `updated` = Now()"
	app, err := oDb.getAppFromNodeAndCandidateApp(ctx, candidateApp, node)
	if err != nil {
		return fmt.Errorf("get application from candidate %s with node_id %s: %w", candidateApp, node.nodeID, err)
	}
	_, err = oDb.db.ExecContext(ctx, query, objectName, node.clusterID, svcID, app, node.nodeEnv, objectName, node.clusterID, app, node.nodeEnv)
	if err != nil {
		return fmt.Errorf("createServiceFromObjectAndCandidateApp %s %s: %w", objectName, svcID, err)
	}
	return nil
}

// findOrCreateObjectID returns uniq svcID for svcname on clusterID. When svcID is not found it creates new svcID row.
// isNew bool is set to true when a new svcID has been allocated.
func (oDb *opensvcDB) findOrCreateObjectID(ctx context.Context, svcname, clusterID string) (isNew bool, svcID string, err error) {
	const (
		queryInsertID = "INSERT IGNORE INTO `service_ids` (`svcname`, `cluster_id`) VALUES (?, ?)"
		querySearchID = "SELECT `svc_id` FROM `service_ids` WHERE `svcname` = ? AND `cluster_id` = ? LIMIT 1"
	)
	var (
		result       sql.Result
		rowsAffected int64
	)
	if result, err = oDb.db.ExecContext(ctx, queryInsertID, svcname, clusterID); err != nil {
		return
	}
	if rowsAffected, err = result.RowsAffected(); err != nil {
		return
	} else if rowsAffected > 0 {
		isNew = true
	}
	if err = oDb.db.QueryRowContext(ctx, querySearchID, svcname, clusterID).Scan(&svcID); err != nil {
		return
	}
	return
}

func (oDb *opensvcDB) appByName(ctx context.Context, app string) (bool, *DBApp, error) {
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

func (oDb *opensvcDB) getAppFromNodeAndCandidateApp(ctx context.Context, candidateApp string, node *DBNode) (string, error) {
	app := candidateApp
	if candidateApp == "" {
		app = node.app
	} else if ok, err := oDb.isAppAllowedForNodeID(ctx, node.nodeID, candidateApp); err != nil {
		return "", fmt.Errorf("can't detect if app %s is allowed: %w", candidateApp, err)
	} else if !ok {
		app = node.app
	}
	if ok, _, err := oDb.appByName(ctx, app); err != nil {
		return "", fmt.Errorf("can't verify guessed app %s: %w", app, err)
	} else if !ok {
		return "", fmt.Errorf("can't verify guessed app %s: app not found", app)
	}
	return app, nil
}

func (oDb *opensvcDB) objectFromID(ctx context.Context, svcID string) (*DBObject, error) {
	const query = "SELECT svcname, svc_id, cluster_id, svc_availstatus FROM services WHERE svc_id = ?"
	var o DBObject
	err := oDb.db.QueryRowContext(ctx, query, svcID).Scan(&o.svcname, &o.svcID, &o.clusterID, &o.availStatus)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, err
	default:
		return &o, nil
	}
}

func (oDb *opensvcDB) tableChange(s ...string) {
	for _, table := range s {
		oDb.tChanges[table] = struct{}{}
	}
}
