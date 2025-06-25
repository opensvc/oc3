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
	DBObject struct {
		svcname   string
		svcID     string
		clusterID string

		DBObjStatus

		env string
		app string

		// nullConfig is true when db svc_config is NULL
		nullConfig bool
	}

	DBObjStatus struct {
		availStatus   string
		overallStatus string
		placement     string
		frozen        string
		provisioned   string
	}

	// DBObjectConfig
	//
	//CREATE TABLE `services` (
	//  `svc_hostid` varchar(30) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
	//  `svcname` varchar(60) DEFAULT NULL,
	//  `svc_nodes` varchar(1000) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
	//  `svc_drpnode` varchar(30) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
	//  `svc_drptype` varchar(7) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
	//  `svc_autostart` varchar(60) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL DEFAULT '',
	//  `svc_env` varchar(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
	//  `svc_drpnodes` varchar(1000) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
	//  `svc_comment` varchar(1000) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
	//  `svc_app` varchar(64) DEFAULT NULL,
	//  `svc_drnoaction` varchar(1) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT 'F',
	//  `svc_created` timestamp NOT NULL DEFAULT current_timestamp(),
	//  `svc_config_updated` datetime DEFAULT NULL,
	//  `svc_metrocluster` varchar(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
	//  `id` int(11) NOT NULL AUTO_INCREMENT,
	//  `svc_wave` varchar(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL DEFAULT '3',
	//  `svc_config` mediumtext DEFAULT NULL,
	//  `updated` datetime NOT NULL,
	//  `svc_topology` varchar(20) DEFAULT 'failover',
	//  `svc_flex_min_nodes` int(11) DEFAULT 1,
	//  `svc_flex_max_nodes` int(11) DEFAULT 0,
	//  `svc_flex_cpu_low_threshold` int(11) DEFAULT 0,
	//  `svc_flex_cpu_high_threshold` int(11) DEFAULT 100,
	//  `svc_status` varchar(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT 'undef',
	//  `svc_availstatus` varchar(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT 'undef',
	//  `svc_ha` tinyint(1) DEFAULT 0,
	//  `svc_status_updated` datetime DEFAULT NULL,
	//  `svc_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
	//  `svc_frozen` varchar(9) DEFAULT NULL,
	//  `svc_provisioned` varchar(6) DEFAULT NULL,
	//  `svc_placement` varchar(12) DEFAULT NULL,
	//  `svc_notifications` varchar(1) DEFAULT 'T',
	//  `svc_snooze_till` datetime DEFAULT NULL,
	//  `cluster_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
	//  `svc_flex_target` int(11) DEFAULT NULL,
	//  PRIMARY KEY (`id`),
	//  UNIQUE KEY `k_svc_id` (`svc_id`),
	//  KEY `svc_hostid` (`svc_hostid`),
	//  KEY `svc_drpnode` (`svc_drpnode`),
	//  KEY `idx2` (`svc_topology`),
	//  KEY `services_svc_app` (`svc_app`),
	//  KEY `k_svc_name` (`svcname`),
	//  KEY `k_cluster_id` (`cluster_id`)
	//) ENGINE=InnoDB AUTO_INCREMENT=2491471 DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci
	DBObjectConfig struct {
		svcID     string
		name      string
		nodes     *string
		clusterID string
		drpNode   *string
		drpNodes  *string
		app       *string
		env       *string
		comment   string
		flexMin   int
		flexMax   int
		ha        bool
		config    string
		updated   time.Time
	}
)

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

func (oDb *opensvcDB) objectIDsFromClusterIDWithPurgeTag(ctx context.Context, clusterID string) (objectIDs []string, err error) {
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

func (oDb *opensvcDB) objectsFromClusterIDAndObjectNames(ctx context.Context, clusterID string, objectNames []string) (dbObjects []*DBObject, err error) {
	defer logDuration("objectsFromClusterIDAndObjectNames", time.Now())
	var query = `
		SELECT svcname, svc_id, cluster_id, svc_availstatus, svc_env, svc_status,
       		svc_placement, svc_provisioned, svc_app, svc_config IS NULL
		FROM services
		WHERE cluster_id = ? AND svcname IN (?`
	if len(objectNames) == 0 {
		err = fmt.Errorf("objectsFromClusterIDAndObjectNames called with empty object name list")
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
		var placement, provisioned, env, app sql.NullString
		var hasConfig sql.NullBool
		if err = rows.Scan(&o.svcname, &o.svcID, &o.clusterID, &o.availStatus, &env, &o.overallStatus, &placement, &provisioned, &app, &hasConfig); err != nil {
			return
		}
		o.placement = placement.String
		o.provisioned = provisioned.String
		o.app = app.String
		o.env = env.String
		o.nullConfig = hasConfig.Bool
		dbObjects = append(dbObjects, &o)
	}
	err = rows.Err()
	return
}

func (oDb *opensvcDB) objectsFromClusterID(ctx context.Context, clusterID string) (dbObjects []*DBObject, err error) {
	defer logDuration("objectsFromClusterID", time.Now())
	var query = `
		SELECT svcname, svc_id, cluster_id, svc_availstatus, svc_env, svc_status,
       		svc_placement, svc_provisioned, svc_app, svc_config IS NULL
		FROM services
		WHERE cluster_id = ?`

	var rows *sql.Rows
	rows, err = oDb.db.QueryContext(ctx, query, clusterID)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var o DBObject
		var placement, provisioned, env, app sql.NullString
		var hasNullConfig sql.NullBool
		if err = rows.Scan(&o.svcname, &o.svcID, &o.clusterID, &o.availStatus, &env, &o.overallStatus, &placement, &provisioned, &app, &hasNullConfig); err != nil {
			return
		}
		o.placement = placement.String
		o.provisioned = provisioned.String
		o.app = app.String
		o.env = env.String
		o.nullConfig = hasNullConfig.Bool
		dbObjects = append(dbObjects, &o)
	}
	err = rows.Err()
	return
}

// objectCreate creates missing object in database and returns *DBObject.
//
// uniq svcID is found thew objectIDFindOrCreate to ensure uniq svcID on concurrent calls
// for same object.
func (oDb *opensvcDB) objectCreate(ctx context.Context, objectName, clusterID, candidateApp string, node *DBNode) (*DBObject, error) {
	created, svcID, err := oDb.objectIDFindOrCreate(ctx, objectName, clusterID)
	if err != nil {
		return nil, fmt.Errorf("can't find or create object: %w", err)
	}
	if created {
		slog.Info(fmt.Sprintf("objectCreate will create service %s@%s with new svc_id: %s", objectName, clusterID, svcID))
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
			return nil, fmt.Errorf("objectCreate %s: can't retrieve service with svc_id %s", objectName, svcID)
		} else {
			return obj, nil
		}
	} else {
		return obj, nil
	}
}

// objectPing updates services.svc_status_updated and services_log_last.svc_end
// when services.svc_status_updated timestamp for svc_id id older than 30s.
func (oDb *opensvcDB) objectPing(ctx context.Context, svcID string) (updates bool, err error) {
	defer logDuration("objectPing "+svcID, time.Now())
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

func (oDb *opensvcDB) objectUpdateStatus(ctx context.Context, svcID string, o *DBObjStatus) error {
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

// objectUpdateLog handle services_log_last and services_log avail value changes.
//
// services_log_last tracks the current avail value from begin to now.
// services_log tracks avail values changes with begin and end: [(avail, begin, end), ...]
func (oDb *opensvcDB) objectUpdateLog(ctx context.Context, svcID string, avail string) error {
	defer logDuration("objectUpdateLog "+svcID, time.Now())
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
			return fmt.Errorf("objectUpdateLog can't update services_log_last %s: %w", svcID, err)
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
		return fmt.Errorf("objectUpdateLog can't get services_log_last %s: %w", svcID, err)
	default:
		defer oDb.tableChange("service_log")
		if previousAvail == avail {
			// no change, extend last interval
			if _, err := oDb.db.ExecContext(ctx, qExtendIntervalOfCurrentAvail, svcID); err != nil {
				return fmt.Errorf("objectUpdateLog can't set services_log_last.svc_end %s: %w", svcID, err)
			}
			return nil
		} else {
			// the avail value will change, save interval of previous avail value before change
			if _, err := oDb.db.ExecContext(ctx, qSaveIntervalOfPreviousAvailBeforeTransition, svcID, previousBegin, previousAvail); err != nil {
				return fmt.Errorf("objectUpdateLog can't save services_log change %s: %w", svcID, err)
			}
			// reset begin and end interval for new avail
			return setLogLast()
		}
	}
}

// insertOrUpdateObjectForNodeAndCandidateApp will insert or update object with svcID.
//
// If candidate app is not valid, node app will be used (see appFromNodeAndCandidateApp)
func (oDb *opensvcDB) insertOrUpdateObjectForNodeAndCandidateApp(ctx context.Context, objectName string, svcID, candidateApp string, node *DBNode) error {
	const query = "" +
		"INSERT INTO `services` (`svcname`, `cluster_id`, `svc_id`, `svc_app`, `svc_env`, `updated`)" +
		" VALUES (?, ?, ?, ?, ?, NOW())" +
		" ON DUPLICATE KEY UPDATE" +
		"    `svcname` = ?, `cluster_id` = ?, `svc_app` = ?, `svc_env` = ?, `updated` = Now()"
	app, err := oDb.appFromNodeAndCandidateApp(ctx, candidateApp, node)
	if err != nil {
		return fmt.Errorf("get application from candidate %s with node_id %s: %w", candidateApp, node.nodeID, err)
	}
	_, err = oDb.db.ExecContext(ctx, query, objectName, node.clusterID, svcID, app, node.nodeEnv, objectName, node.clusterID, app, node.nodeEnv)
	if err != nil {
		return fmt.Errorf("createServiceFromObjectAndCandidateApp %s %s: %w", objectName, svcID, err)
	}
	return nil
}

// insertOrUpdateObjectConfig will insert or update object config with svcID.
func (oDb *opensvcDB) insertOrUpdateObjectConfig(ctx context.Context, c *DBObjectConfig) (bool, error) {
	const query = "" +
		"INSERT INTO `services` (`svcname`, `cluster_id`, `svc_id`, `svc_nodes`, `svc_drpnode`, `svc_drpnodes`" +
		" ,  `svc_app`, `svc_env`, `svc_comment`, `svc_flex_min_nodes`, `svc_flex_max_nodes`" +
		" , `svc_ha`, `svc_config`, `updated`)" +
		" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())" +
		" ON DUPLICATE KEY UPDATE  `svcname`=VALUES(`svcname`), `cluster_id`=VALUES(`cluster_id`)" + "" +
		"   , `svc_nodes`=VALUES(`svc_nodes`), `svc_drpnode`=VALUES(`svc_drpnode`), `svc_drpnodes`=VALUES(`svc_drpnodes`)" +
		"   , `svc_app`=VALUES(`svc_app`), `svc_env`=VALUES(`svc_env`), `svc_comment`=VALUES(`svc_comment`)" + "" +
		"   , `svc_flex_min_nodes`=VALUES(`svc_flex_min_nodes`), `svc_flex_max_nodes`=VALUES(`svc_flex_max_nodes`)" +
		"   , `svc_ha`=VALUES(`svc_ha`), `svc_config`=VALUES(`svc_config`), `updated`=VALUES(`updated`)"
	var nodes, drpNode, drpNodes, app, env any
	if c.nodes != nil {
		nodes = *c.nodes
	}
	if c.drpNodes != nil {
		drpNodes = *c.drpNodes
	}
	if c.drpNode != nil {
		drpNode = *c.drpNode
	}
	if c.app != nil {
		app = *c.app
	}
	if c.env != nil {
		env = *c.env
	}
	result, err := oDb.db.ExecContext(ctx, query, c.name, c.clusterID, c.svcID, nodes, drpNode, drpNodes,
		app, env, c.comment, c.flexMin, c.flexMax, c.ha, c.config)
	if err != nil {
		return false, fmt.Errorf("update services config: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return false, fmt.Errorf("update services config unable to count row affected: %w", err)
	} else {
		return affected > 0, nil
	}
}

// objectIDFindOrCreate returns uniq svcID for svcname on clusterID. When svcID is not found it creates new svcID row.
// isNew bool is set to true when a new svcID has been allocated.
func (oDb *opensvcDB) objectIDFindOrCreate(ctx context.Context, svcname, clusterID string) (isNew bool, svcID string, err error) {
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

func (oDb *opensvcDB) purgeTablesFromObjectID(ctx context.Context, id string) error {
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
