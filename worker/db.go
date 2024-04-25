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
		availStatus   string
		overallStatus string
		placement     string
		frozen        string
		provisioned   string
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

func (oDb *opensvcDB) updateObjectStatus(ctx context.Context, svcID string, o *DBObjStatus) error {
	const query = "" +
		"UPDATE `services` SET `svc_availstatus` = ?" +
		" , `svc_status` = ?" +
		" , `svc_placement` = ?" +
		" , `svc_frozen` = ?" +
		" , `svc_provisioned` = ?" +
		" , `svc_status_updated` = NOW()" +
		" WHERE `svc_id`= ? "
	if _, err := oDb.db.ExecContext(ctx, query, o.availStatus, o.overallStatus, o.placement, o.frozen, o.provisioned, svcID); err != nil {
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
	const query = "SELECT svcname, svc_id, cluster_id, svc_availstatus, svc_status, svc_frozen, svc_placement, svc_provisioned FROM services WHERE svc_id = ?"
	var o DBObject
	var frozen, placement, provisioned sql.NullString
	err := oDb.db.QueryRowContext(ctx, query, svcID).Scan(&o.svcname, &o.svcID, &o.clusterID, &o.availStatus,
		&o.overallStatus, &frozen, &placement, &provisioned)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, err
	default:
		o.frozen = placement.String
		o.placement = placement.String
		o.provisioned = placement.String
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

func (oDb *opensvcDB) tableChanges() []string {
	var r []string
	for s := range oDb.tChanges {
		r = append(r, s)
	}
	return r
}

func (oDb *opensvcDB) updateTableModified(ctx context.Context, tableName string) error {
	defer logDuration("updateTableModified", time.Now())
	const (
		query = "" +
			"INSERT INTO `table_modified` VALUES (NULL, ?, NOW())" +
			" ON DUPLICATE KEY UPDATE `table_modified` = NOW()"
	)
	_, err := oDb.db.ExecContext(ctx, query, tableName)
	if err != nil {
		return fmt.Errorf("updateTableModified %s: %w", tableName, err)
	}
	return nil
}
