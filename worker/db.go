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

	// DBInstanceStatus
	//
	// CREATE TABLE `svcmon` (
	//  `mon_svctype` varchar(10) DEFAULT NULL,
	//  `mon_ipstatus` varchar(10) DEFAULT 'undef',
	//  `mon_fsstatus` varchar(10) DEFAULT 'undef',
	//  `mon_updated` datetime DEFAULT NULL,
	//  `ID` int(11) NOT NULL AUTO_INCREMENT,
	//  `mon_frozen` int(11) DEFAULT NULL,
	//  `mon_changed` timestamp NOT NULL DEFAULT current_timestamp(),
	//  `mon_diskstatus` varchar(10) DEFAULT 'undef',
	//  `mon_containerstatus` varchar(10) DEFAULT 'undef',
	//  `mon_overallstatus` varchar(10) DEFAULT 'undef',
	//  `mon_syncstatus` varchar(10) DEFAULT 'undef',
	//  `mon_appstatus` varchar(10) DEFAULT 'undef',
	//  `mon_hbstatus` varchar(10) DEFAULT NULL,
	//  `mon_availstatus` varchar(10) DEFAULT 'undef',
	//  `mon_vmname` varchar(50) DEFAULT '',
	//  `mon_guestos` varchar(30) DEFAULT NULL,
	//  `mon_vmem` int(11) DEFAULT 0,
	//  `mon_vcpus` float DEFAULT 0,
	//  `mon_containerpath` varchar(512) DEFAULT NULL,
	//  `mon_vmtype` varchar(10) DEFAULT NULL,
	//  `mon_sharestatus` varchar(10) DEFAULT 'undef',
	//  `node_id` char(36) CHARACTER SET ascii DEFAULT '',
	//  `svc_id` char(36) CHARACTER SET ascii DEFAULT '',
	//  `mon_smon_status` varchar(32) DEFAULT NULL,
	//  `mon_smon_global_expect` varchar(32) DEFAULT NULL,
	//  PRIMARY KEY (`ID`),
	//  UNIQUE KEY `uk_svcmon` (`node_id`,`svc_id`,`mon_vmname`),
	//  KEY `mon_vmname` (`mon_vmname`),
	//  KEY `k_node_id` (`node_id`),
	//  KEY `k_svc_id` (`svc_id`)
	//) ENGINE=InnoDB AUTO_INCREMENT=28468 DEFAULT CHARSET=utf8
	DBInstanceStatus struct {
		ID                  int64
		nodeID              string
		svcID               string
		monVmName           string
		monSmonStatus       string
		monSmonGlobalExpect string
		monAvailStatus      string
		monOverallStatus    string
		monIpStatus         string
		monDiskStatus       string
		monFsStatus         string
		monShareStatus      string
		monContainerStatus  string
		monAppStatus        string
		monSyncStatus       string
		monFrozen           int
		monVmType           string
		monUpdated          string
	}

	// DBInstanceResource is the database table resmon
	//
	// CREATE TABLE `resmon` (
	//        `id` int(11) NOT NULL AUTO_INCREMENT,
	//        `rid` varchar(255) DEFAULT NULL,
	//        `res_status` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
	//        `changed` timestamp NOT NULL DEFAULT current_timestamp(),
	//        `updated` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
	//        `res_desc` text DEFAULT NULL,
	//        `res_log` text DEFAULT NULL,
	//        `vmname` varchar(60) DEFAULT '',
	//        `res_monitor` varchar(1) DEFAULT NULL,
	//        `res_disable` varchar(1) DEFAULT NULL,
	//        `res_optional` varchar(1) DEFAULT NULL,
	//        `res_type` varchar(16) DEFAULT NULL,
	//        `node_id` char(36) CHARACTER SET ascii DEFAULT '',
	//        `svc_id` char(36) CHARACTER SET ascii DEFAULT '',
	//
	//        PRIMARY KEY (`id`),
	//
	//        UNIQUE KEY `uk_resmon_1` (`svc_id`,`node_id`,`vmname`,`rid`),
	//
	//        KEY `resmon_updated` (`updated`),
	//        KEY `k_node_id` (`node_id`),
	//        KEY `k_svc_id` (`svc_id`),
	//        KEY `idx_node_id_updated` (`node_id`,`updated`)
	// ) ENGINE=InnoDB AUTO_INCREMENT=15524822 DEFAULT CHARSET=utf8
	DBInstanceResource struct {
		svcID    string
		nodeID   string
		vmName   string
		rid      string
		status   string
		changed  time.Time
		updated  time.Time
		desc     string
		log      string
		monitor  string
		disable  string
		optional string
		resType  string
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
		db DBOperater

		tChanges map[string]struct{}
	}

	DBTxer interface {
		Commit() error
		Rollback() error
	}

	DBOperater interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
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
	defer logDuration("pingObject "+svcID, time.Now())
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
	defer logDuration("pingInstance "+svcID+"@"+nodeID, time.Now())
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
	begin := time.Now()
	err = oDb.db.QueryRowContext(ctx, qHasInstance, svcID, nodeID).Scan(&count)
	slog.Debug(fmt.Sprintf("pingInstance qHasInstance %s", time.Now().Sub(begin)))
	begin = time.Now()

	switch {
	case errors.Is(err, sql.ErrNoRows):
		err = nil
		return
	case err != nil:
		return
	}

	begin = time.Now()
	if result, err = oDb.db.ExecContext(ctx, qUpdateSvcmon, svcID, nodeID); err != nil {
		return
	} else if count, err = result.RowsAffected(); err != nil {
		return
	} else if count == 0 {
		return
	}
	slog.Debug(fmt.Sprintf("pingInstance qUpdateSvcmon %s", time.Now().Sub(begin)))
	begin = time.Now()
	updates = true

	oDb.tableChange("svcmon")
	if _, err = oDb.db.ExecContext(ctx, qUpdateSvcmonLogLast, svcID, nodeID); err != nil {
		return
	}
	slog.Debug(fmt.Sprintf("pingInstance qUpdateSvcmonLogLast %s", time.Now().Sub(begin)))
	begin = time.Now()

	if result, err = oDb.db.ExecContext(ctx, qUpdateResmon, svcID, nodeID); err != nil {
		return
	} else if count, err = result.RowsAffected(); err != nil {
		return
	} else if count == 0 {
		return
	}
	slog.Debug(fmt.Sprintf("pingInstance qUpdateResmon %s", time.Now().Sub(begin)))
	begin = time.Now()
	oDb.tableChange("resmon")

	if _, err = oDb.db.ExecContext(ctx, qUpdateResmonLogLast, svcID, nodeID); err != nil {
		return
	}

	slog.Debug(fmt.Sprintf("pingInstance qUpdateResmonLogLast %s", time.Now().Sub(begin)))
	begin = time.Now()
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
	defer logDuration("updateObjectLog "+svcID, time.Now())
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
		qGetLogLast = "SELECT `svc_availstatus`, `svc_begin` FROM `services_log_last` WHERE `svc_id` = ?"
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

		previousBegin time.Time
	)
	setLogLast := func() error {
		_, err := oDb.db.ExecContext(ctx, qSetLogLast, svcID, avail, avail)
		if err != nil {
			return fmt.Errorf("updateObjectLog can't update services_log_last %s: %w", svcID, err)
		}
		return nil
	}
	err := oDb.db.QueryRowContext(ctx, qGetLogLast, svcID).Scan(&previousAvail, &previousBegin)
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

func (oDb *opensvcDB) translateEncapNodename(ctx context.Context, svcID, nodeID string) (subNodeID, vmName, vmType string, err error) {
	const (
		query = "" +
			"SELECT `svcmon`.`node_id`, `svcmon`.`mon_vmname`, `svcmon`.`mon_vmtype`, `svcmon`.`mon_containerstatus`" +
			" FROM `nodes`, `svcmon`" +
			" WHERE (" +
			"   ((`svcmon`.`mon_vmname` = `nodes`.`nodename`)" +
			"    AND (`nodes`.`node_id` = ?)" +
			"   ) AND (`svcmon`.`svc_id` = ?))"
	)
	var (
		rows    *sql.Rows
		hasRow1 bool

		containerStatusMatch = []string{"up", "stdby up", "n/a"}
	)

	rows, err = oDb.db.QueryContext(ctx, query, nodeID, svcID)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var subNodeIDT, vmNameT, vmTypeT, containerStatusT string
		if err = rows.Scan(&subNodeIDT, &vmNameT, &vmTypeT, &containerStatusT); err != nil {
			return
		}
		for _, v := range containerStatusMatch {
			if containerStatusT == v {
				subNodeID = subNodeIDT
				vmName = vmNameT
				vmType = vmTypeT
				return
			}
		}
		if !hasRow1 {
			hasRow1 = true
			subNodeID = subNodeIDT
			vmName = vmNameT
			vmType = vmTypeT
		}
	}
	err = rows.Err()
	return
}

func (oDb *opensvcDB) tableChange(s ...string) {
	for _, table := range s {
		oDb.tChanges[table] = struct{}{}
	}
}

func (oDb *opensvcDB) instanceStatusDelete(ctx context.Context, svcID, nodeID string) error {
	defer logDuration("instanceStatusDelete "+svcID+"@"+nodeID, time.Now())
	const (
		queryDelete = "" +
			"DELETE FROM `svcmon` WHERE `svc_id` = ? AND `node_id` = ?"
	)
	result, err := oDb.db.ExecContext(ctx, queryDelete, svcID, nodeID)
	if err != nil {
		return fmt.Errorf("instanceStatusDelete %s@%s: %w", svcID, nodeID, err)
	}
	if changes, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("instanceStatusDelete %s@%s can't get deleted count: %w", svcID, nodeID, err)
	} else if changes > 0 {
		oDb.tableChange("svcmon")
	}
	return nil
}

func (oDb *opensvcDB) instanceStatusLogUpdate(ctx context.Context, status *DBInstanceStatus) error {
	defer logDuration("instanceStatusLogUpdate "+status.svcID+"@"+status.nodeID, time.Now())
	/*
		CREATE TABLE `svcmon_log_last` (
		  `id` int(11) NOT NULL AUTO_INCREMENT,
		  `mon_overallstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		  `mon_ipstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		  `mon_fsstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		  `mon_diskstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		  `mon_containerstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		  `mon_syncstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		  `mon_appstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		  `mon_begin` datetime NOT NULL,
		  `mon_end` datetime NOT NULL,
		  `mon_hbstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		  `mon_availstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		  `mon_sharestatus` varchar(10) DEFAULT 'undef',
		  `node_id` char(36) CHARACTER SET ascii DEFAULT '',
		  `svc_id` char(36) CHARACTER SET ascii DEFAULT '',
		  PRIMARY KEY (`id`),
		  UNIQUE KEY `uk` (`node_id`,`svc_id`),
		  KEY `mon_overallstatus` (`mon_overallstatus`),
		  KEY `mon_begin` (`mon_begin`,`mon_end`),
		  KEY `k_node_id` (`node_id`),
		  KEY `k_svc_id` (`svc_id`)
		) ENGINE=InnoDB AUTO_INCREMENT=15639 DEFAULT CHARSET=utf8

		CREATE TABLE `svcmon_log` (
		  `id` int(11) NOT NULL AUTO_INCREMENT,
		  `mon_overallstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		  `mon_ipstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		  `mon_fsstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		  `mon_diskstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		  `mon_containerstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		  `mon_syncstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		  `mon_appstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		  `mon_begin` datetime NOT NULL,
		  `mon_end` datetime NOT NULL,
		  `mon_hbstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		  `mon_availstatus` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		  `mon_sharestatus` varchar(10) DEFAULT 'undef',
		  `node_id` char(36) CHARACTER SET ascii DEFAULT '',
		  `svc_id` char(36) CHARACTER SET ascii DEFAULT '',
		  PRIMARY KEY (`id`),
		  KEY `mon_overallstatus` (`mon_overallstatus`),
		  KEY `mon_begin` (`mon_begin`,`mon_end`),
		  KEY `k_node_id` (`node_id`),
		  KEY `k_svc_id` (`svc_id`)
		) ENGINE=InnoDB AUTO_INCREMENT=6749866 DEFAULT CHARSET=utf8
	*/
	const (
		queryGetLogLast = "" +
			"SELECT `mon_begin`, `id`, " +
			" `mon_availstatus`, `mon_overallstatus`, `mon_syncstatus`, `mon_ipstatus`, `mon_fsstatus`," +
			" `mon_diskstatus`, `mon_sharestatus`, `mon_containerstatus`, `mon_appstatus`" +
			"FROM `svcmon_log_last`" +
			"WHERE `svc_id` = ? AND `node_id` = ?"

		querySetLogLast = "" +
			"INSERT INTO `svcmon_log_last` (`svc_id`, `node_id`," +
			" `mon_availstatus`, `mon_overallstatus`, `mon_syncstatus`, `mon_ipstatus`, `mon_fsstatus`," +
			" `mon_diskstatus`, `mon_sharestatus`, `mon_containerstatus`, `mon_appstatus`," +
			" `mon_begin`, `mon_end`) " +
			" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())" +
			" ON DUPLICATE KEY UPDATE " +
			"   `mon_availstatus` = ?, `mon_overallstatus` = ? , `mon_syncstatus` = ?, `mon_ipstatus` = ?, `mon_fsstatus` = ?," +
			" `mon_diskstatus` = ?, `mon_sharestatus` = ?, `mon_containerstatus` = ?, `mon_appstatus`= ?," +
			" `mon_begin` = NOW(), `mon_end` = NOW()"

		queryExtendIntervalOfCurrent = "" +
			"UPDATE `svcmon_log_last` SET `mon_end` = NOW() " +
			" WHERE `svc_id` = ? AND `node_id` = ?"

		querySaveIntervalOfPreviousBeforeTransition = "" +
			"INSERT INTO `svcmon_log` (`svc_id`, `node_id`," +
			" `mon_availstatus`, `mon_overallstatus`, `mon_syncstatus`, `mon_ipstatus`, `mon_fsstatus`," +
			" `mon_diskstatus`, `mon_sharestatus`, `mon_containerstatus`, `mon_appstatus`," +
			" `mon_begin`, `mon_end`)" +
			" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())"
	)
	var (
		prev DBInstanceStatus

		previousBegin time.Time
	)
	setLogLast := func() error {
		_, err := oDb.db.ExecContext(ctx, querySetLogLast, status.svcID, status.nodeID,
			status.monAvailStatus, status.monOverallStatus, status.monSyncStatus, status.monIpStatus, status.monFsStatus,
			status.monDiskStatus, status.monShareStatus, status.monContainerStatus, status.monAppStatus,
			status.monAvailStatus, status.monOverallStatus, status.monSyncStatus, status.monIpStatus, status.monFsStatus,
			status.monDiskStatus, status.monShareStatus, status.monContainerStatus, status.monAppStatus)
		if err != nil {
			return fmt.Errorf("update svcmon_log_last: %w", err)
		}
		return nil
	}
	err := oDb.db.QueryRowContext(ctx, queryGetLogLast, status.svcID, status.nodeID).Scan(&previousBegin, &prev.ID,
		&prev.monAvailStatus, &prev.monOverallStatus, &prev.monSyncStatus, &prev.monIpStatus, &prev.monFsStatus,
		&prev.monDiskStatus, &prev.monShareStatus, &prev.monContainerStatus, &prev.monAppStatus)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// set initial status, log value
		defer oDb.tableChange("svcmon_log")
		return setLogLast()
	case err != nil:
		return fmt.Errorf("get svcmon_log_last: %w", err)
	default:
		defer oDb.tableChange("svcmon_log")
		if status.monAvailStatus == prev.monAvailStatus &&
			status.monOverallStatus == prev.monOverallStatus &&
			status.monSyncStatus == prev.monSyncStatus &&
			status.monIpStatus == prev.monIpStatus &&
			status.monFsStatus == prev.monFsStatus &&
			status.monDiskStatus == prev.monDiskStatus &&
			status.monShareStatus == prev.monShareStatus &&
			status.monContainerStatus == prev.monContainerStatus &&
			status.monAppStatus == prev.monAppStatus {
			// no change, extend last interval
			if _, err := oDb.db.ExecContext(ctx, queryExtendIntervalOfCurrent, status.svcID, status.nodeID); err != nil {
				return fmt.Errorf("extend svcmon_log_last: %w", err)
			}
			return nil
		} else {
			// the avail value will change, save interval of prev status, log value before change
			_, err := oDb.db.ExecContext(ctx, querySaveIntervalOfPreviousBeforeTransition, status.svcID, status.nodeID,
				prev.monAvailStatus, prev.monOverallStatus, prev.monSyncStatus, prev.monIpStatus, prev.monFsStatus,
				prev.monDiskStatus, prev.monShareStatus, prev.monContainerStatus, prev.monAppStatus,
				previousBegin)
			if err != nil {
				return fmt.Errorf("save svcmon_log transition: %w", err)
			}
			// reset previousBegin and end interval for new status, log
			return setLogLast()
		}
	}
}

func (oDb *opensvcDB) instanceStatusUpdate(ctx context.Context, s *DBInstanceStatus) error {
	defer logDuration("instanceStatusUpdate "+s.svcID+"@"+s.nodeID, time.Now())
	const (
		qUpdate = "" +
			"INSERT INTO `svcmon` (`svc_id`, `node_id`, `mon_vmname`, " +
			" `mon_smon_status`, `mon_smon_global_expect`, `mon_availstatus`, " +
			" `mon_overallstatus`, `mon_ipstatus`, `mon_diskstatus`, `mon_fsstatus`," +
			" `mon_sharestatus`, `mon_containerstatus`, `mon_appstatus`, `mon_syncstatus`," +
			" `mon_frozen`, `mon_vmtype`, `mon_updated`)" +
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())" +
			"ON DUPLICATE KEY UPDATE" +
			" `mon_smon_status` = ?, `mon_smon_global_expect` = ?, `mon_availstatus` = ?, " +
			" `mon_overallstatus` = ?, `mon_ipstatus` = ?, `mon_diskstatus` = ?, `mon_fsstatus` = ?," +
			" `mon_sharestatus` = ?, `mon_containerstatus` = ?, `mon_appstatus` = ?, `mon_syncstatus` = ?," +
			" `mon_frozen` = ?, `mon_vmtype` = ?, `mon_updated` = NOW()"
	)
	// TODO check vs v2
	_, err := oDb.db.ExecContext(ctx, qUpdate, s.svcID, s.nodeID, s.monVmName,
		s.monSmonStatus, s.monSmonGlobalExpect, s.monAvailStatus,
		s.monOverallStatus, s.monIpStatus, s.monDiskStatus, s.monFsStatus,
		s.monShareStatus, s.monContainerStatus, s.monAppStatus, s.monSyncStatus,
		s.monFrozen, s.monVmType,
		s.monSmonStatus, s.monSmonGlobalExpect, s.monAvailStatus,
		s.monOverallStatus, s.monIpStatus, s.monDiskStatus, s.monFsStatus,
		s.monShareStatus, s.monContainerStatus, s.monAppStatus, s.monSyncStatus,
		s.monFrozen, s.monVmType)
	if err != nil {
		return err
	}
	oDb.tableChange("svcmon")
	return nil
}

func (oDb *opensvcDB) instanceResourcesDelete(ctx context.Context, svcID, nodeID string) error {
	defer logDuration("instanceResourcesDelete "+svcID+"@"+nodeID+":", time.Now())
	const (
		queryPurge = "DELETE FROM `resmon` WHERE `svc_id` = ? AND `node_id` = ?"
	)
	if _, err := oDb.db.ExecContext(ctx, queryPurge, svcID, nodeID); err != nil {
		return fmt.Errorf("instanceResourcesDelete: %w", err)
	}
	return nil
}

func (oDb *opensvcDB) instanceResourcesDeleteObsolete(ctx context.Context, svcID, nodeID string, maxTime time.Time) error {
	defer logDuration("instanceResourcesDeleteObsolete "+svcID+"@"+nodeID+":", time.Now())
	const (
		queryPurge = "" +
			"DELETE FROM `resmon`" +
			"WHERE `svc_id` = ? AND `node_id` = ? AND `updated` < ?"
	)
	result, err := oDb.db.ExecContext(ctx, queryPurge, svcID, nodeID, maxTime)
	if err != nil {
		return fmt.Errorf("instanceResourcesDeleteObsolete: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("instanceResourcesDeleteObsolete count affected: %w", err)
	} else {
		slog.Debug(fmt.Sprintf("instanceResourcesDeleteObsolete %s@%s: %d", svcID, nodeID, affected))
	}
	return nil
}

// instanceResourceLogUpdate handle resmon_log_last and resmon_log avail value changes.
//
// resmon_log_last tracks the current status, log value from begin to now.
// resmon_log tracks status, log values changes with begin and end: [(status, log, begin, end), ...]
func (oDb *opensvcDB) instanceResourceLogUpdate(ctx context.Context, res *DBInstanceResource) error {
	defer logDuration("instanceResourceLogUpdate "+res.svcID+"@"+res.nodeID+":"+res.rid, time.Now())
	/*
		CREATE TABLE `resmon_log` (
		  `id` int(11) NOT NULL AUTO_INCREMENT,
		  `node_id` char(36) CHARACTER SET ascii DEFAULT '',
		  `svc_id` char(36) CHARACTER SET ascii DEFAULT '',
		  `rid` varchar(255) NOT NULL,
		  `res_status` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		  `res_begin` datetime NOT NULL,
		  `res_end` datetime NOT NULL,
		  `res_log` text DEFAULT NULL,
		  PRIMARY KEY (`id`),
		  KEY `idx1` (`node_id`,`svc_id`,`rid`),
		  KEY `idx_res_end` (`res_end`)
		) ENGINE=InnoDB AUTO_INCREMENT=1079229 DEFAULT CHARSET=utf8

		CREATE TABLE `resmon_log_last` (
		  `id` int(11) NOT NULL AUTO_INCREMENT,
		  `node_id` char(36) CHARACTER SET ascii DEFAULT '',
		  `svc_id` char(36) CHARACTER SET ascii DEFAULT '',
		  `rid` varchar(255) NOT NULL,
		  `res_status` enum('up','down','warn','n/a','undef','stdby up','stdby down') DEFAULT 'undef',
		  `res_begin` datetime NOT NULL,
		  `res_end` datetime NOT NULL,
		  `res_log` text DEFAULT NULL,
		  PRIMARY KEY (`id`),
		  UNIQUE KEY `uk` (`node_id`,`svc_id`,`rid`),
		  KEY `idx1` (`node_id`,`svc_id`,`rid`)
		) ENGINE=InnoDB AUTO_INCREMENT=40132 DEFAULT CHARSET=utf8
	*/
	const (
		queryGetLogLast = "" +
			"SELECT `res_status`, `res_log`, `res_begin`" +
			" FROM `resmon_log_last` WHERE `svc_id` = ? AND `node_id` = ? AND `rid` = ?"
		querySetLogLast = "" +
			"INSERT INTO `resmon_log_last` (`svc_id`, `node_id`, `rid`," +
			" `res_begin`, `res_end`, `res_status`, `res_log`)" +
			" VALUES (?, ?, ?, NOW(), NOW(), ?, ?)" +
			" ON DUPLICATE KEY UPDATE " +
			"   `res_begin` = NOW(), `res_end` = NOW(), `res_status`= ?, `res_log` = ?"
		queryExtendIntervalOfCurrent = "" +
			"UPDATE `resmon_log_last` SET `res_end` = NOW() " +
			" WHERE `svc_id` = ? AND `node_id` = ? AND `rid` = ?"
		querySaveIntervalOfPreviousBeforeTransition = "" +
			"INSERT INTO `resmon_log` (`svc_id`, `node_id`, `rid`, `res_begin`, `res_end`, `res_status`, `res_log`)" +
			" VALUES (?, ?, ?, ?, NOW(), ?, ?)"
	)
	var (
		previousStatus, previousLog string

		previousBegin time.Time
	)
	setLogLast := func() error {
		_, err := oDb.db.ExecContext(ctx, querySetLogLast,
			res.svcID, res.nodeID, res.rid,
			res.status, res.log,
			res.status, res.log)
		if err != nil {
			return fmt.Errorf("update resmon_log_last: %w", err)
		}
		return nil
	}
	err := oDb.db.QueryRowContext(ctx, queryGetLogLast, res.svcID, res.nodeID, res.rid).Scan(&previousStatus, &previousLog, &previousBegin)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// set initial status, log value
		defer oDb.tableChange("resmon_log")
		return setLogLast()
	case err != nil:
		return fmt.Errorf("get resmon_log_last: %w", err)
	default:
		defer oDb.tableChange("resmon_log")
		if previousStatus == res.status && previousLog == res.log {
			// no change, extend last interval
			if _, err := oDb.db.ExecContext(ctx, queryExtendIntervalOfCurrent, res.svcID, res.nodeID, res.rid); err != nil {
				return fmt.Errorf("extend services_log_last: %w", err)
			}
			return nil
		} else {
			// the avail value will change, save interval of previous status, log value before change
			if _, err := oDb.db.ExecContext(ctx, querySaveIntervalOfPreviousBeforeTransition,
				res.svcID, res.nodeID, res.rid, previousBegin, previousStatus, previousLog); err != nil {
				return fmt.Errorf("add services_log change: %w", err)
			}
			// reset begin and end interval for new status, log
			return setLogLast()
		}
	}
}

func (oDb *opensvcDB) instanceResourceUpdate(ctx context.Context, res *DBInstanceResource) error {
	defer logDuration("instanceResourceUpdate "+res.svcID+"@"+res.nodeID+":"+res.rid, time.Now())
	const (
		query = "" +
			"INSERT INTO `resmon` (`svc_id`, `node_id`, `vmname`, `rid`," +
			" `res_status`, `res_type`, `res_log`, `res_desc`," +
			" `res_optional`, `res_disable`, `res_monitor`," +
			" `updated`)" +
			"VALUES (?, ?, ?, ?," +
			" ?, ?, ?, ?," +
			" ?, ?, ?, NOW())" +
			"ON DUPLICATE KEY UPDATE" +
			" `res_status` = ?, `res_type` = ?, `res_log` = ?,  `res_desc` = ?," +
			" `res_optional` = ?, `res_disable` = ?, `res_monitor` = ?," +
			" `updated` = NOW()"
	)
	_, err := oDb.db.ExecContext(ctx, query,
		res.svcID, res.nodeID, res.vmName, res.rid,
		res.status, res.resType, res.log, res.desc,
		res.optional, res.disable, res.monitor,
		res.status, res.resType, res.log, res.desc,
		res.disable, res.disable, res.monitor,
	)

	if err != nil {
		return fmt.Errorf("instanceResourceUpdate %s: %w", res.rid, err)
	}
	oDb.tableChange("resmon")
	return nil
}

func (oDb *opensvcDB) deleteByObject(ctx context.Context, tableName string, objID string) error {
	const (
		query = "DELETE from ? WHERE svc_id = ?"
	)
	if result, err := oDb.db.ExecContext(ctx, query, tableName, objID); err != nil {
		return fmt.Errorf("deleteByObject: %w", err)
	} else if count, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("deleteByObject affected: %w", err)
	} else if count > 0 {
		oDb.tableChange(tableName)
	}
	return nil
}

func (oDb *opensvcDB) deleteByInstanceID(ctx context.Context, tableName string, objID, nodeId string) error {
	const (
		query = "DELETE from ? WHERE svc_id = ? AND node_id = ?"
	)
	if result, err := oDb.db.ExecContext(ctx, query, tableName, objID, nodeId); err != nil {
		return fmt.Errorf("deleteByInstanceID: %w", err)
	} else if count, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("deleteByInstanceID affected: %w", err)
	} else if count > 0 {
		oDb.tableChange(tableName)
	}
	return nil
}

// getOrphanInstances returns list of InstanceID defined on svcmon for nodeIDs but where
// the associated services.svcname is not into objectNames
//
//	SELECT `svcmon`.`svc_id`, `svcmon`.`node_id`
//	FROM `services`, `svcmon`
//	WHERE
//			`svcmon`.`node_id` IN ('nodeID1','nodeID2')
//		AND
//	    	`svcmon`.`svc_id` = `services`.`svc_id`
//		AND
//			`services`.`svcname` NOT IN ('obj1','obj2',...)
func (oDb *opensvcDB) getOrphanInstances(ctx context.Context, nodeIDs, objectNames []string) (instanceIDs []InstanceID, err error) {
	var (
		query = "SELECT `svcmon`.`svc_id`, `svcmon`.`node_id` FROM `services`, `svcmon`"

		queryArgs []any

		rows *sql.Rows
	)

	if len(nodeIDs) == 0 || len(objectNames) == 0 {
		return
	}
	query += " WHERE `svcmon`.`node_id` IN (?"
	queryArgs = append(queryArgs, nodeIDs[0])
	for i := 1; i < len(nodeIDs); i++ {
		query += ", ?"
		queryArgs = append(queryArgs, nodeIDs[i])
	}
	query += " ) AND `svcmon`.`svc_id` = `services`.`svc_id`"
	query += " AND `services`.`svcname` NOT IN ( ?"
	queryArgs = append(queryArgs, objectNames[0])
	for i := 1; i < len(objectNames); i++ {
		query += ", ?"
		queryArgs = append(queryArgs, objectNames[i])
	}
	query += " )"
	rows, err = oDb.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var instanceID InstanceID
		if err = rows.Scan(&instanceID.svcID, &instanceID.nodeID); err != nil {
			return
		}
		instanceIDs = append(instanceIDs, instanceID)
	}
	err = rows.Err()
	return
}

func (oDb *opensvcDB) purgeInstances(ctx context.Context, id InstanceID) error {
	const (
		where = "WHERE `svc_id` = ? and `node_id` = ?"
	)

	var (
		tables = []string{
			"svcmon", "dashboard", "dashboard_events", "svcdisks", "resmon",
			"checks_live", "comp_status", "action_queue", "resinfo", "saves",
		}

		err error
	)
	slog.Debug(fmt.Sprintf("purging instance %s", id))
	for _, tableName := range tables {
		request := fmt.Sprintf("DELETE FROM %s WHERE `svc_id` = ? and `node_id` = ?", tableName)
		result, err1 := oDb.db.ExecContext(ctx, request, id.svcID, id.nodeID)
		if err1 != nil {
			err = errors.Join(err, fmt.Errorf("delete from %s: %w", tableName, err1))
			continue
		}
		if rowAffected, err1 := result.RowsAffected(); err1 != nil {
			err = errors.Join(err, fmt.Errorf("count delete from %s: %w", tableName, err1))
		} else if rowAffected > 0 {
			slog.Debug(fmt.Sprintf("purged table %s instance %s", tableName, id))
			oDb.tableChange(tableName)
		}
	}
	return err
}

func (oDb *opensvcDB) objectIDWithPurgeTag(ctx context.Context, clusterID string) (objectIDs []string, err error) {
	const (
		query = "" +
			"SELECT `svc_tags`.`svc_id`" +
			" FROM `tags`, `services`, `svc_tags`" +
			" LEFT JOIN `svcmon` ON `svc_tags`.`svc_id` = `svcmon`.`svc_id`" +
			" WHERE" +
			"   `services`.`svc_id`=`svc_tags`.`svc_id`" +
			"   AND `services`.`cluster_id` = ?" +
			"   AND `tags`.`tag_id` = `svc_tags`.`tag_id`" +
			"   AND `tags`.`tag_name` = '@purge'" +
			"   AND `svcmon`.`id` IS NULL"
	)
	var (
		rows *sql.Rows
	)
	rows, err = oDb.db.QueryContext(ctx, query, clusterID)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var ID string
		if err = rows.Scan(&ID); err != nil {
			return
		}
		objectIDs = append(objectIDs, ID)
	}
	err = rows.Err()
	return
}

func (oDb *opensvcDB) purgeObject(ctx context.Context, id string) error {
	const (
		where = "WHERE `svc_id` = ?"
	)

	var (
		tables = []string{
			"services", "svcactions", "drpservices", "svcmon_log", "resmon_log",
			"svcmon_log_ack", "checks_settings", "comp_log", "comp_log_daily",
			"comp_rulesets_services", "comp_modulesets_services", "log",
			"action_queue", "svc_tags", "form_output_results", "svcmon_log_last",
			"resmon_log_last",
		}

		err error
	)
	slog.Debug(fmt.Sprintf("purging object %s", id))
	for _, tableName := range tables {
		request := fmt.Sprintf("DELETE FROM %s WHERE `svc_id` = ?", tableName)
		result, err1 := oDb.db.ExecContext(ctx, request, id)
		if err1 != nil {
			err = errors.Join(err, fmt.Errorf("delete from %s: %w", tableName, err1))
			continue
		}
		if rowAffected, err1 := result.RowsAffected(); err1 != nil {
			err = errors.Join(err, fmt.Errorf("count delete from %s: %w", tableName, err1))
		} else if rowAffected > 0 {
			slog.Debug(fmt.Sprintf("purged table %s object %s", tableName, id))
			oDb.tableChange(tableName)
		}
	}
	return err
}
