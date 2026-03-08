package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
)

type (
	ObjectMeta struct {
		ID   int64
		OID  uuid.UUID
		Name string
	}

	/*
		DBObject is a subset of the db table `services`

		is the db table `services`
			CREATE TABLE `services` (
			  `svc_hostid` varchar(30) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
			  `svcname` varchar(60) DEFAULT NULL,
			  `svc_nodes` varchar(1000) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
			  `svc_drpnode` varchar(30) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
			  `svc_drptype` varchar(7) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
			  `svc_autostart` varchar(60) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL DEFAULT '',
			  `svc_env` varchar(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
			  `svc_drpnodes` varchar(1000) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
			  `svc_comment` varchar(1000) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
			  `svc_app` varchar(64) DEFAULT NULL,
			  `svc_drnoaction` varchar(1) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT 'F',
			  `svc_created` timestamp NOT NULL DEFAULT current_timestamp(),
			  `svc_config_updated` datetime DEFAULT NULL,
			  `svc_metrocluster` varchar(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `svc_wave` varchar(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL DEFAULT '3',
			  `svc_config` mediumtext DEFAULT NULL,
			  `updated` datetime NOT NULL,
			  `svc_topology` varchar(20) DEFAULT 'failover',
			  `svc_flex_min_nodes` int(11) DEFAULT 1,
			  `svc_flex_max_nodes` int(11) DEFAULT 0,
			  `svc_flex_cpu_low_threshold` int(11) DEFAULT 0,
			  `svc_flex_cpu_high_threshold` int(11) DEFAULT 100,
			  `svc_status` varchar(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT 'undef',
			  `svc_availstatus` varchar(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT 'undef',
			  `svc_ha` tinyint(1) DEFAULT 0,
			  `svc_status_updated` datetime DEFAULT NULL,
			  `svc_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
			  `svc_frozen` varchar(9) DEFAULT NULL,
			  `svc_provisioned` varchar(6) DEFAULT NULL,
			  `svc_placement` varchar(12) DEFAULT NULL,
			  `svc_notifications` varchar(1) DEFAULT 'T',
			  `svc_snooze_till` datetime DEFAULT NULL,
			  `cluster_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
			  `svc_flex_target` int(11) DEFAULT NULL,
			  PRIMARY KEY (`id`),
			  UNIQUE KEY `k_svc_id` (`svc_id`),
			  KEY `svc_hostid` (`svc_hostid`),
			  KEY `svc_drpnode` (`svc_drpnode`),
			  KEY `idx2` (`svc_topology`),
			  KEY `services_svc_app` (`svc_app`),
			  KEY `k_svc_name` (`svcname`),
			  KEY `k_cluster_id` (`cluster_id`)
			) ENGINE=InnoDB AUTO_INCREMENT=2491471 DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci
	*/
	DBObject struct {
		Svcname   string
		SvcID     string
		ClusterID string

		DBObjStatus

		Env string
		App string

		// NullConfig is true when db svc_config is NULL
		NullConfig bool

		// The `services.updated` column tracks the last time the service configuration
		Updated time.Time
	}

	DBObjStatus struct {
		AvailStatus   string
		OverallStatus string
		Placement     string
		Frozen        string
		Provisioned   string
	}

	// DBObjectConfig is the configuration for a service object (a subset of the db table `services`)
	DBObjectConfig struct {
		SvcID     string
		Name      string
		Nodes     *string
		ClusterID string
		DrpNode   *string
		DrpNodes  *string
		App       *string
		Env       *string
		Comment   string
		FlexMin   int
		FlexMax   int
		HA        bool
		Config    string
		Updated   time.Time
	}

	DBObjectStatusLog struct {
		SvcID       string
		AvailStatus string
		BeginAt     time.Time
		EndAt       time.Time
	}
)

func (t ObjectMeta) String() string {
	if t.Name != "" {
		return t.Name
	} else {
		return t.OID.String()
	}
}

func (oDb *DB) ObjectFromID(ctx context.Context, svcID string) (*DBObject, error) {
	const query = "SELECT svcname, svc_id, cluster_id, svc_availstatus, svc_status, svc_frozen, svc_placement, svc_provisioned, updated FROM `services` WHERE svc_id = ?"
	var o DBObject
	var frozen, placement, provisioned sql.NullString
	err := oDb.DB.QueryRowContext(ctx, query, svcID).Scan(&o.Svcname, &o.SvcID, &o.ClusterID, &o.AvailStatus,
		&o.OverallStatus, &frozen, &placement, &provisioned, &o.Updated)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, err
	default:
		o.Frozen = placement.String
		o.Placement = placement.String
		o.Provisioned = placement.String
		return &o, nil
	}
}

func (oDb *DB) ObjectIDsFromClusterIDWithPurgeTag(ctx context.Context, clusterID string) (objectIDs []string, err error) {
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
	rows, err = oDb.DB.QueryContext(ctx, query, clusterID)
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

func (oDb *DB) ObjectsFromClusterIDAndObjectNames(ctx context.Context, clusterID string, objectNames []string) (dbObjects []*DBObject, err error) {
	defer logDuration("objectsFromClusterIDAndObjectNames", time.Now())
	var query = `
		SELECT svcname, svc_id, cluster_id, svc_availstatus, svc_env, svc_status,
       		svc_placement, svc_provisioned, svc_app, svc_config IS NULL, updated
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
	rows, err = oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var o DBObject
		var placement, provisioned, env, app sql.NullString
		var hasConfig sql.NullBool
		if err = rows.Scan(&o.Svcname, &o.SvcID, &o.ClusterID, &o.AvailStatus, &env, &o.OverallStatus, &placement, &provisioned, &app, &hasConfig, &o.Updated); err != nil {
			return
		}
		o.Placement = placement.String
		o.Provisioned = provisioned.String
		o.App = app.String
		o.Env = env.String
		o.NullConfig = hasConfig.Bool
		dbObjects = append(dbObjects, &o)
	}
	err = rows.Err()
	return
}

func (oDb *DB) ObjectsFromClusterID(ctx context.Context, clusterID string) (dbObjects []*DBObject, err error) {
	defer logDuration("objectsFromClusterID", time.Now())
	var query = `
		SELECT svcname, svc_id, cluster_id, svc_availstatus, svc_env, svc_status,
       		svc_placement, svc_provisioned, svc_app, svc_config IS NULL, updated
		FROM services
		WHERE cluster_id = ?`

	var rows *sql.Rows
	rows, err = oDb.DB.QueryContext(ctx, query, clusterID)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var o DBObject
		var placement, provisioned, env, app sql.NullString
		var hasNullConfig sql.NullBool
		if err = rows.Scan(&o.Svcname, &o.SvcID, &o.ClusterID, &o.AvailStatus, &env, &o.OverallStatus, &placement, &provisioned, &app, &hasNullConfig, &o.Updated); err != nil {
			return
		}
		o.Placement = placement.String
		o.Provisioned = provisioned.String
		o.App = app.String
		o.Env = env.String
		o.NullConfig = hasNullConfig.Bool
		dbObjects = append(dbObjects, &o)
	}
	err = rows.Err()
	return
}

// ObjectCreate creates missing object in database and returns *DBObject.
//
// uniq svcID is found thew objectIDFindOrCreate to ensure uniq svcID on concurrent calls
// for same object.
func (oDb *DB) ObjectCreate(ctx context.Context, objectName, clusterID, candidateApp string, node *DBNode) (*DBObject, error) {
	created, svcID, err := oDb.ObjectIDFindOrCreate(ctx, objectName, clusterID)
	if err != nil {
		return nil, fmt.Errorf("can't find or create object: %w", err)
	}
	if created {
		// TODO: add metrics
		slog.Debug(fmt.Sprintf("objectCreate will create service %s@%s with new svc_id: %s", objectName, clusterID, svcID))
		if err := oDb.insertOrUpdateObjectForNodeAndCandidateApp(ctx, objectName, svcID, candidateApp, node); err != nil {
			return nil, err
		}
	}
	if obj, err := oDb.ObjectFromID(ctx, svcID); err != nil {
		return nil, err
	} else if obj == nil {
		// svc id exists without associated service
		slog.Debug(fmt.Sprintf("will create service %s with existing svc_id: %s", objectName, svcID))
		if err := oDb.insertOrUpdateObjectForNodeAndCandidateApp(ctx, objectName, svcID, candidateApp, node); err != nil {
			return nil, err
		}
		if obj, err := oDb.ObjectFromID(ctx, svcID); err != nil {
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

// ObjectPing updates services.svc_status_updated and services_log_last.svc_end
func (oDb *DB) ObjectsPing(ctx context.Context, ids []string) (updates bool, err error) {
	defer logDuration("ObjectsPing", time.Now())
	const UpdateServicesSvcStatusUpdated = "UPDATE `services` SET `svc_status_updated` = NOW() WHERE `svc_id` IN (%s)"
	const updateSvcLogLastSvc = "UPDATE `services_log_last` SET `svc_end` = NOW() WHERE `svc_id` IN (%s)"
	var count int64

	if len(ids) > 0 {
		return false, nil
	}
	placeholders := strings.Repeat("?,", len(ids)-1) + "?"

	query := fmt.Sprintf(UpdateServicesSvcStatusUpdated, placeholders)
	args := make([]any, len(ids))
	for i, v := range ids {
		args[i] = v
	}
	begin := time.Now()
	if count, err = oDb.execCountContext(ctx, query, args...); err != nil {
		return
	} else if count > 0 {
		updates = true
		oDb.SetChange("services")
	}
	slog.Info(fmt.Sprintf("STAT: %s elapse: %s", "UpdateServicesSvcStatusUpdated", time.Since(begin)))

	begin = time.Now()
	query = fmt.Sprintf(updateSvcLogLastSvc, placeholders)
	if count, err = oDb.execCountContext(ctx, query, args...); err != nil {
		return
	} else if count > 0 {
		updates = true
		oDb.SetChange("services")
	}
	slog.Info(fmt.Sprintf("STAT: %s elapse: %s", "updateSvcLogLastSvc", time.Since(begin)))

	return
}

func (oDb *DB) ObjectStatusUpdate(ctx context.Context, l ...*DBObject) error {
	defer logDuration("ObjectStatusUpdate", time.Now())
	const (
		insertColList = "(`svc_id`,`svc_availstatus`,`svc_status`,`svc_placement`,`svc_frozen`,`svc_provisioned`,`svc_status_updated`, `updated`)"

		valueList = "(?, ?, ?, ?, ?, ?, NOW(), ?)"

		onDuplicateAssignment = "" +
			"`svc_availstatus`=VALUES(`svc_availstatus`)," +
			"`svc_status`=VALUES(`svc_status`)," +
			"`svc_placement`=VALUES(`svc_placement`)," +
			"`svc_frozen`=VALUES(`svc_frozen`)," +
			"`svc_provisioned`=VALUES(`svc_provisioned`)," +
			"`svc_status_updated`=VALUES(`svc_status_updated`)," +
			"`updated`=VALUES(`updated`)"
	)
	if len(l) == 0 {
		return nil
	}
	placeholders := strings.Repeat(valueList+", ", len(l)-1) + valueList
	query := fmt.Sprintf("INSERT INTO `services` %s VALUES %s ON DUPLICATE KEY UPDATE %s", insertColList, placeholders, onDuplicateAssignment)

	args := make([]any, 0, 7*len(l))
	for _, o := range l {
		args = append(args, o.SvcID, o.AvailStatus, o.OverallStatus, o.Placement, o.Frozen, o.Provisioned, o.Updated)
	}
	_, err := oDb.ExecContext(ctx, query, args...)
	if err != nil {
		oDb.SetChange("services")
	}
	return err
}

// ObjectUpdateLog handle services_log_last and services_log avail value changes.
//
// services_log_last tracks the current avail value from begin to now.
// services_log tracks avail values changes with begin and end: [(avail, begin, end), ...]
func (oDb *DB) ObjectUpdateLog(ctx context.Context, svcID string, avail string) error {
	defer logDuration("ObjectUpdateLog "+svcID, time.Now())
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
		_, err := oDb.ExecContext(ctx, qSetLogLast, svcID, avail, avail)
		if err != nil {
			return fmt.Errorf("objectUpdateLog can't update services_log_last %s: %w", svcID, err)
		}
		return nil
	}
	err := oDb.DB.QueryRowContext(ctx, qGetLogLast, svcID).Scan(&previousAvail, &previousBegin)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// set initial avail value
		defer oDb.SetChange("services_log")
		return setLogLast()
	case err != nil:
		return fmt.Errorf("objectUpdateLog can't get services_log_last %s: %w", svcID, err)
	default:
		defer oDb.SetChange("services_log")
		if previousAvail == avail {
			// no change, extend last interval
			if _, err := oDb.ExecContext(ctx, qExtendIntervalOfCurrentAvail, svcID); err != nil {
				return fmt.Errorf("objectUpdateLog can't set services_log_last.svc_end %s: %w", svcID, err)
			}
			return nil
		} else {
			// the avail value will change, save interval of previous avail value before change
			if _, err := oDb.ExecContext(ctx, qSaveIntervalOfPreviousAvailBeforeTransition, svcID, previousBegin, previousAvail); err != nil {
				return fmt.Errorf("objectUpdateLog can't save services_log change %s: %w", svcID, err)
			}
			// reset begin and end interval for new avail
			return setLogLast()
		}
	}
}

func (oDb *DB) ObjectStatusLogLastFromObjectIDs(ctx context.Context, objectIDs ...string) ([]*DBObjectStatusLog, error) {
	if len(objectIDs) == 0 {
		return nil, nil
	}
	var (
		querySelect = "SELECT `svc_id`, `svc_availstatus`, `svc_begin`, `svc_end` FROM `services_log_last` WHERE `svc_id` IN (%s)"
		args        = make([]any, len(objectIDs))

		l = make([]*DBObjectStatusLog, 0)
	)

	if len(objectIDs) == 0 {
		return nil, nil
	}
	placeholders := strings.Repeat("?,", len(objectIDs)-1) + "?"
	query := fmt.Sprintf(querySelect, placeholders)

	for i, v := range objectIDs {
		args[i] = v
	}

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, fmt.Errorf("got nil rows result")
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var r DBObjectStatusLog
		if err := rows.Scan(&r.SvcID, &r.AvailStatus, &r.BeginAt, &r.EndAt); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		l = append(l, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows err: %w", err)
	}
	return l, nil
}

func (oDb *DB) ObjectStatusLogLastUpdate(ctx context.Context, l ...*DBObjectStatusLog) error {
	defer logDuration("ObjectStatusLogLastUpdate", time.Now())
	const (
		insertColList         = "(`svc_id`, `svc_availstatus`, `svc_begin`, `svc_end`)"
		valueList             = "(?, ?, NOW(), NOW())"
		onDuplicateAssignment = "`svc_availstatus` = VALUES(`svc_availstatus`), `svc_begin` = VALUES(`svc_begin`), `svc_end` = VALUES(`svc_end`)"
	)
	if len(l) == 0 {
		return nil
	}
	placeholders := strings.Repeat(valueList+", ", len(l)-1) + valueList

	query := fmt.Sprintf("INSERT INTO `services_log_last` %s VALUES %s ON DUPLICATE KEY UPDATE %s", insertColList, placeholders, onDuplicateAssignment)
	args := make([]any, 0, 2*len(l))
	for _, v := range l {
		args = append(args, v.SvcID, v.AvailStatus)
	}

	_, err := oDb.ExecContext(ctx, query, args...)
	return err
}

func (oDb *DB) ObjectStatusLogLastExtend(ctx context.Context, objectIDs ...string) error {
	defer logDuration("ObjectStatusLogLastExtend", time.Now())
	if len(objectIDs) == 0 {
		return nil
	}
	placeholders := strings.Repeat("?,", len(objectIDs)-1) + "?"

	query := fmt.Sprintf("UPDATE `services_log_last` SET `svc_end` = NOW() WHERE `svc_id` in (%s)", placeholders)
	args := make([]any, len(objectIDs))
	for i, v := range objectIDs {
		args[i] = v
	}

	_, err := oDb.ExecContext(ctx, query, args...)
	return err
}

func (oDb *DB) ObjectStatusLogUpdate(ctx context.Context, l ...*DBObjectStatusLog) error {
	defer logDuration("ObjectStatusLogUpdate", time.Now())
	const (
		insertColList = "(`svc_id`, `svc_availstatus`, `svc_begin`, `svc_end`)"
		valueList     = "(?, ?, ?, NOW())"
	)
	if len(l) == 0 {
		return nil
	}
	placeholders := strings.Repeat(valueList+", ", len(l)-1) + valueList

	query := fmt.Sprintf("INSERT INTO `services_log` %s VALUES %s", insertColList, placeholders)
	args := make([]any, 0, 3*len(l))
	for _, v := range l {
		args = append(args, v.SvcID, v.AvailStatus, v.BeginAt)
	}

	_, err := oDb.ExecContext(ctx, query, args...)
	return err
}

// insertOrUpdateObjectForNodeAndCandidateApp will insert or update object with svcID.
//
// If candidate app is not valid, node app will be used (see appFromNodeAndCandidateApp)
func (oDb *DB) insertOrUpdateObjectForNodeAndCandidateApp(ctx context.Context, objectName string, svcID, candidateApp string, node *DBNode) error {
	const query = "" +
		"INSERT INTO `services` (`svcname`, `cluster_id`, `svc_id`, `svc_app`, `svc_env`, `updated`)" +
		" VALUES (?, ?, ?, ?, ?, NOW())" +
		" ON DUPLICATE KEY UPDATE" +
		"    `svcname` = ?, `cluster_id` = ?, `svc_app` = ?, `svc_env` = ?, `updated` = Now()"
	app, err := oDb.AppFromNodeAndCandidateApp(ctx, candidateApp, node)
	if err != nil {
		return fmt.Errorf("get application from candidate %s with node_id %s: %w", candidateApp, node.NodeID, err)
	}
	_, err = oDb.ExecContext(ctx, query, objectName, node.ClusterID, svcID, app, node.NodeEnv, objectName, node.ClusterID, app, node.NodeEnv)
	if err != nil {
		return fmt.Errorf("createServiceFromObjectAndCandidateApp %s %s: %w", objectName, svcID, err)
	}
	return nil
}

// InsertOrUpdateObjectConfig will insert or update object config with svcID.
func (oDb *DB) InsertOrUpdateObjectConfig(ctx context.Context, c *DBObjectConfig) (bool, error) {
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
	if c.Nodes != nil {
		nodes = *c.Nodes
	}
	if c.DrpNodes != nil {
		drpNodes = *c.DrpNodes
	}
	if c.DrpNode != nil {
		drpNode = *c.DrpNode
	}
	if c.App != nil {
		app = *c.App
	}
	if c.Env != nil {
		env = *c.Env
	}
	count, err := oDb.execCountContext(ctx, query, c.Name, c.ClusterID, c.SvcID, nodes, drpNode, drpNodes, app, env, c.Comment, c.FlexMin, c.FlexMax, c.HA, c.Config)
	if err != nil {
		return false, fmt.Errorf("update services config: %w", err)
	}
	return count > 0, nil
}

// ObjectIDFindOrCreate ensures a unique svc_id exists for a given svcname and clusterID, creating it if necessary.
// Returns a boolean indicating if a new ID was created, the svc_id, or an error if the operation fails.
// It is run inside a locked separate transaction to ensure always success insertion.
func (oDb *DB) ObjectIDFindOrCreate(ctx context.Context, svcname, clusterID string) (isNew bool, svcID string, err error) {
	const (
		queryInsertID = "INSERT IGNORE INTO `service_ids` (`svcname`, `cluster_id`) VALUES (?, ?)"
		querySearchID = "SELECT `svc_id` FROM `service_ids` WHERE `svcname` = ? AND `cluster_id` = ? LIMIT 1"
	)
	var (
		result       sql.Result
		rowsAffected int64
	)
	oDb.DBLck.Lock()
	defer oDb.DBLck.Unlock()

	tx, err1 := oDb.DBLck.DB.BeginTx(ctx, nil)
	if err1 != nil {
		err = fmt.Errorf("can't begin transaction: %w", err1)
		return
	}
	if result, err = tx.ExecContext(ctx, queryInsertID, svcname, clusterID); err != nil {
		err = fmt.Errorf("INSERT IGNORE INTO `service_ids`: %w", err)
		_ = tx.Rollback()
		return
	} else if result == nil {
		err = fmt.Errorf("INSERT IGNORE INTO `service_ids` returned nil result")
		_ = tx.Rollback()
		return
	}
	if rowsAffected, err = result.RowsAffected(); err != nil {
		err = fmt.Errorf("count row affected for INSERT IGNORE INTO `service_ids`: %w", err)
		_ = tx.Rollback()
		return
	} else if rowsAffected > 0 {
		isNew = true
	}
	if err = tx.QueryRowContext(ctx, querySearchID, svcname, clusterID).Scan(&svcID); err != nil {
		err = fmt.Errorf("retrieve service id failed:%w", err)
		_ = tx.Rollback()
		return
	}
	err = tx.Commit()
	return
}

func (oDb *DB) PurgeTablesFromObjectID(ctx context.Context, id string) error {
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
		if count, err1 := oDb.execCountContext(ctx, request, id); err1 != nil {
			err = errors.Join(err, fmt.Errorf("delete from %s: %w", tableName, err1))
			continue
		} else if count > 0 {
			slog.Debug(fmt.Sprintf("purged table %s object %s", tableName, id))
			oDb.SetChange(tableName)
		}
	}
	return err
}

// ObjectsOutdated return lists of ids, svc_ids and svcnames for objects that no
// longer have instances updated in the last 15 minutes and that don't have their object
// status set to "undef" yet.
func (oDb *DB) ObjectsOutdated(ctx context.Context) (objects []ObjectMeta, err error) {
	sql := `SELECT id, svc_id, svcname FROM services
                WHERE svc_id IN (SELECT svc_id FROM v_outdated_services WHERE uptodate=0)
                AND (svc_status != "undef" OR svc_availstatus != "undef")`
	rows, err := oDb.DB.QueryContext(ctx, sql)
	if err != nil {
		return
	}
	defer rows.Close()

	for {
		next := rows.Next()
		if !next {
			break
		}
		var o ObjectMeta
		rows.Scan(&o.ID, &o.OID, &o.Name)
		objects = append(objects, o)
	}
	return
}

func (oDb *DB) ObjectUpdateStatusSimple(ctx context.Context, objects []ObjectMeta, availStatus, overallStatus string) (int64, error) {
	idsLen := len(objects)
	sql := `UPDATE services
                SET svc_status=?, svc_availstatus=?, svc_status_updated=NOW()
                WHERE id IN (%s)`
	sql = fmt.Sprintf(sql, Placeholders(idsLen))

	args := make([]any, idsLen+2)
	args[0] = overallStatus
	args[1] = availStatus
	for i, o := range objects {
		args[i+2] = o.ID
	}

	if count, err := oDb.execCountContext(ctx, sql, args...); err != nil {
		return 0, err
	} else if count > 0 {
		oDb.Session.SetChanges("services")
		return count, nil
	} else {
		return 0, nil
	}
}

func (o *DBObject) SameAsLog(logStatus *DBObjectStatusLog) bool {
	if logStatus == nil {
		return false
	}
	return o.AvailStatus == logStatus.AvailStatus
}

func (o *DBObject) AsLog() *DBObjectStatusLog {
	return &DBObjectStatusLog{
		SvcID:       o.SvcID,
		AvailStatus: o.AvailStatus,
	}
}
