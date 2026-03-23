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
	ResourceMeta struct {
		ID  int64
		OID uuid.UUID
		NID uuid.UUID
		RID string
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
		SvcID  string
		NodeID string
		VmName string
		RID    string
		Status string
		// Changed is refreshed when value is changed (see  DBInstanceResource.SameAsLog()
		Changed time.Time
		// Updated is refreshed each time the resource is analysed
		Updated  time.Time
		Desc     string
		Log      string
		Monitor  string
		Disable  string
		Optional string
		ResType  string
	}

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
	DBResmonLog struct {
		SvcID  string
		NodeID string
		RID    string

		Status string
		Log    string

		BeginAt time.Time
		EndAt   time.Time
	}

	// DBInstanceResinfo is the database table resinfo
	//
	// CREATE TABLE `resinfo` (
	//  `id` int(11) NOT NULL AUTO_INCREMENT,
	//  `rid` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
	//  `res_key` varchar(40) DEFAULT '',
	//  `res_value` varchar(255) DEFAULT NULL,
	//  `updated` timestamp NOT NULL DEFAULT current_timestamp(),
	//  `topology` varchar(20) DEFAULT 'failover',
	//  `node_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
	//  `svc_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
	//  PRIMARY KEY (`id`),
	//  UNIQUE KEY `uk` (`node_id`,`svc_id`,`rid`,`res_key`),
	//  KEY `k_node_id` (`node_id`),
	//  KEY `k_svc_id` (`svc_id`)
	//) ENGINE=InnoDB AUTO_INCREMENT=175883380 DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci
	DBInstanceResinfo struct {
		SvcID    string
		NodeID   string
		Topology string
		Updated  time.Time
		RID      string
		Key      string
		Value    string
	}
)

func (t ResourceMeta) String() string {
	return fmt.Sprintf("%s@%s:%s", t.OID, t.NID, t.RID)
}

// ResmonRefreshTimestamp updates resmon.updated and resmon_log_last.res_end for object ids with node id.
func (oDb *DB) ResmonRefreshTimestamp(ctx context.Context, nodeID string, objectIDs ...string) (updates bool, err error) {
	defer logDuration("ResmonRefreshTimestamp", time.Now())
	const (
		qUpdateResmon        = "UPDATE `resmon` SET `updated` = NOW() WHERE `node_id` = ? AND `svc_id` IN (%s)"
		qUpdateResmonLogLast = "UPDATE `resmon_log_last` SET `res_end` = NOW() WHERE `node_id` = ? AND `svc_id` IN (%s)"
	)
	var (
		count int64
		query string
	)
	begin := time.Now()
	if len(objectIDs) == 0 {
		return
	}
	placeholders := strings.Repeat("?,", len(objectIDs)-1) + "?"
	args := make([]any, len(objectIDs)+1)
	args[0] = nodeID
	for i, v := range objectIDs {
		args[i+1] = v
	}

	query = fmt.Sprintf(qUpdateResmon, placeholders)
	if count, err = oDb.execCountContext(ctx, query, args...); err != nil {
		return
	} else if count > 0 {
		updates = true
		oDb.SetChange("resmon")
		oDb.Metrics.ResourceStatusUpdate.Add(float64(count))
	}

	query = fmt.Sprintf(qUpdateResmonLogLast, placeholders)
	if _, err = oDb.ExecContext(ctx, query, args...); err != nil {
		return
	}

	slog.Info(fmt.Sprintf("ResmonRefreshTimestamp %s", time.Since(begin)))
	return
}

func (oDb *DB) ResourceOutdatedLists(ctx context.Context) (resources []ResourceMeta, err error) {
	sql := `SELECT id, rid, svc_id, node_id FROM resmon
                WHERE updated < DATE_SUB(NOW(), INTERVAL 15 MINUTE)
                AND res_status != "undef"`
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
		var resource ResourceMeta
		rows.Scan(&resource.ID, &resource.RID, &resource.OID, &resource.NID)
		resources = append(resources, resource)
	}
	return
}

// ResourceUpdateLog handle resmon_log_last and resmon_log avail value changes.
//
// resmon_log_last tracks the current avail value from begin to now.
// resmon_log tracks avail values changes with begin and end: [(avail, begin, end), ...]
func (oDb *DB) ResourceUpdateLog(ctx context.Context, resource ResourceMeta, avail string) error {
	defer logDuration("ResourceUpdateLog "+resource.String(), time.Now())
	const (
		qGetLogLast = "SELECT `res_status`, `res_begin` FROM `resmon_log_last` WHERE `svc_id` = ? AND `node_id` = ? AND `rid` = ?"
		qSetLogLast = "" +
			"INSERT INTO `resmon_log_last` (`svc_id`, `node_id`, `rid`, `res_begin`, `res_end`, `res_status`)" +
			" VALUES (?, ?, ?, NOW(), NOW(), ?)" +
			" ON DUPLICATE KEY UPDATE `res_begin` = NOW(), `res_end` = NOW(), `res_status` = ?"
		qExtendIntervalOfCurrentAvail                = "UPDATE `resmon_log_last` SET `res_end` = NOW() WHERE `svc_id` = ? AND `node_id` = ? AND `rid` = ?"
		qSaveIntervalOfPreviousAvailBeforeTransition = "" +
			"INSERT INTO `resmon_log` (`svc_id`, `node_id`, `rid`, `res_begin`, `res_end`, `res_status`)" +
			" VALUES (?, ?, ?, ?, NOW(), ?)"
	)
	var (
		previousAvail string

		previousBegin time.Time
	)
	setLogLast := func() error {
		_, err := oDb.ExecContext(ctx, qSetLogLast, resource.OID, resource.NID, resource.RID, avail, avail)
		if err != nil {
			return fmt.Errorf("objectUpdateLog can't update resmon_log_last %s: %w", resource, err)
		}
		return nil
	}
	err := oDb.DB.QueryRowContext(ctx, qGetLogLast, resource.OID, resource.NID, resource.RID).Scan(&previousAvail, &previousBegin)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// set initial avail value
		defer oDb.SetChange("resmon_log")
		return setLogLast()
	case err != nil:
		return fmt.Errorf("objectUpdateLog can't get resmon_log_last %s: %w", resource, err)
	default:
		defer oDb.SetChange("resmon_log")
		if previousAvail == avail {
			// no change, extend last interval
			if count, err := oDb.execCountContext(ctx, qExtendIntervalOfCurrentAvail, resource.OID, resource.NID, resource.RID); err != nil {
				return fmt.Errorf("objectUpdateLog can't set resmon_log_last.res_end %s: %w", resource, err)
			} else if count > 0 {
				oDb.Metrics.ObjectStatusLogExtend.Inc()
			}
			return nil
		} else {
			// the avail value will change, save interval of previous avail value before change
			if count, err := oDb.execCountContext(ctx, qSaveIntervalOfPreviousAvailBeforeTransition, resource.OID, resource.NID, resource.RID, previousBegin, previousAvail); err != nil {
				return fmt.Errorf("objectUpdateLog can't save resmon_log change %s: %w", resource, err)
			} else if count > 0 {
				oDb.Metrics.ObjectStatusLogChange.Inc()
			}
			// reset begin and end interval for new avail
			return setLogLast()
		}
	}
}

func (oDb *DB) ResourceUpdateStatus(ctx context.Context, resources []ResourceMeta, status string) (n int64, err error) {
	idsLen := len(resources)
	sql := `UPDATE resmon
                SET res_status=?, updated=NOW()
                WHERE id IN (%s)`
	sql = fmt.Sprintf(sql, Placeholders(idsLen))

	args := make([]any, idsLen+1)
	args[0] = status
	for i, resource := range resources {
		args[i+1] = resource.ID
	}

	if n, err = oDb.execCountContext(ctx, sql, args...); err != nil {
		return 0, err
	} else if n > 0 {
		oDb.Session.SetChanges("resmon")
	}
	return n, err
}

func (oDb *DB) PurgeResmonOutdated(ctx context.Context) error {
	var query = `DELETE
		FROM resmon
		WHERE
		  updated < DATE_SUB(NOW(), INTERVAL 1 DAY)`
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("resmon")
	}
	return nil
}

func (oDb *DB) ResmonLogLastFromObjectIDs(ctx context.Context, objectIDs ...string) ([]*DBResmonLog, error) {
	if len(objectIDs) == 0 {
		return nil, nil
	}
	var (
		querySelect = "SELECT `svc_id`, `node_id`, `rid`, `res_status`, `res_log`, `res_begin`, `res_end` FROM `resmon_log_last` WHERE `svc_id` IN (%s)"
		args        = make([]any, len(objectIDs))

		l = make([]*DBResmonLog, 0)
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
		return nil, fmt.Errorf("query returns nil rows")
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var r DBResmonLog
		if err := rows.Scan(&r.SvcID, &r.NodeID, &r.RID, &r.Status, &r.Log, &r.BeginAt, &r.EndAt); err != nil {
			return nil, fmt.Errorf("query scan rows: %w", err)
		}
		l = append(l, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("query rows error: %w", err)
	}
	return l, nil
}

func (oDb *DB) ResmonLogLastUpdate(ctx context.Context, l ...*DBResmonLog) error {
	defer logDuration("ResmonLogLastUpdate", time.Now())
	const (
		insertColList         = "(`svc_id`, `node_id`, `rid`, `res_begin`, `res_end`, `res_status`, `res_log`)"
		valueList             = "(?, ?, ?, NOW(), NOW(), ?, ?)"
		onDuplicateAssignment = "`res_begin`=VALUES(`res_begin`), `res_end`=VALUES(`res_end`), `res_status`=VALUES(`res_status`), `res_log`=VALUES(`res_log`)"
	)
	if len(l) == 0 {
		return nil
	}
	placeholders := strings.Repeat(valueList+", ", len(l)-1) + valueList

	query := fmt.Sprintf("INSERT INTO `resmon_log_last` %s VALUES %s ON DUPLICATE KEY UPDATE %s", insertColList, placeholders, onDuplicateAssignment)
	args := make([]any, 0, 5*len(l))
	for _, v := range l {
		args = append(args, v.SvcID, v.NodeID, v.RID, v.Status, v.Log)
	}

	_, err := oDb.ExecContext(ctx, query, args...)
	return err
}

func (oDb *DB) ResmonLogLastExtend(ctx context.Context, l ...*DBInstanceResource) error {
	defer logDuration("ResmonLogLastExtend", time.Now())
	if len(l) == 0 {
		return nil
	}
	placeholders := strings.Repeat("(?,?,?),", len(l)-1) + "(?,?,?)"

	query := fmt.Sprintf("UPDATE `resmon_log_last` SET `res_end` = NOW() WHERE (`svc_id`, `node_id`, `rid`) in (%s)", placeholders)
	args := make([]any, 0, 3*len(l))
	for _, v := range l {
		args = append(args, v.SvcID, v.NodeID, v.RID)
	}

	if count, err := oDb.execCountContext(ctx, query, args...); err != nil {
		return err
	} else if count > 0 {
		oDb.Metrics.ResourceStatusLogExtend.Add(float64(count))
	}
	return nil
}

func (oDb *DB) ResmonLogUpdate(ctx context.Context, l ...*DBResmonLog) error {
	defer logDuration("ResmonLogUpdate", time.Now())
	const (
		insertColList = "(`svc_id`, `node_id`, `rid`, `res_begin`, `res_end`, `res_status`, `res_log`)"
		valueList     = "(?, ?, ?, ?, NOW(), ?, ?)"
	)
	if len(l) == 0 {
		return nil
	}
	placeholders := strings.Repeat(valueList+", ", len(l)-1) + valueList

	query := fmt.Sprintf("INSERT INTO `resmon_log` %s VALUES %s", insertColList, placeholders)
	args := make([]any, 0, 6*len(l))
	for _, v := range l {
		args = append(args, v.SvcID, v.NodeID, v.RID, v.BeginAt, v.Status, v.Log)
	}

	if count, err := oDb.execCountContext(ctx, query, args...); err != nil {
		return err
	} else if count > 0 {
		oDb.Metrics.ResourceStatusLogChange.Add(float64(count))
	}
	return nil
}

func (oDb *DB) ResmonUpdate(ctx context.Context, l ...*DBInstanceResource) error {
	defer logDuration("ResmonUpdate", time.Now())
	const (
		insertColList         = "(`svc_id`,`node_id`,`vmname`,`rid`,`res_status`,`res_type`,`res_log`,`res_desc`,`res_optional`,`res_disable`,`res_monitor`, `changed`, `updated`)"
		valueList             = "(?,?,?,?,?,?,?,?,?,?,?,?,NOW())"
		onDuplicateAssignment = "" +
			"`svc_id`=VALUES(`svc_id`), `node_id`=VALUES(`node_id`), `vmname`=VALUES(`vmname`), `rid`=VALUES(`rid`), `res_status`=VALUES(`res_status`)," +
			"`res_type`=VALUES(`res_type`), `res_log`=VALUES(`res_log`), `res_desc`=VALUES(`res_desc`), `res_optional`=VALUES(`res_optional`)," +
			"`res_disable`=VALUES(`res_disable`), `res_monitor`=VALUES(`res_monitor`),`changed`=VALUES(`changed`),`updated`=VALUES(`updated`)"
	)
	if len(l) == 0 {
		return nil
	}
	placeholders := strings.Repeat(valueList+", ", len(l)-1) + valueList

	query := fmt.Sprintf("INSERT INTO `resmon` %s VALUES %s ON DUPLICATE KEY UPDATE %s", insertColList, placeholders, onDuplicateAssignment)
	args := make([]any, 0, 12*len(l))
	for _, v := range l {
		args = append(args, v.SvcID, v.NodeID, v.VmName, v.RID, v.Status, v.ResType, v.Log, v.Desc, v.Optional, v.Disable, v.Monitor, v.Changed)
	}

	if count, err := oDb.execCountContext(ctx, query, args...); err != nil {
		return err
	} else if count > 0 {
		oDb.Metrics.ResourceStatusUpdate.Add(float64(count))
	}
	return nil
}

func (oDb *DB) ResmonPurgeExpired(ctx context.Context, maxTime time.Time, l ...string) error {
	defer logDuration("ResmonPurgeExpired", time.Now())
	if len(l) == 0 {
		return nil
	}
	placeholders := strings.Repeat("?,", len(l)-1) + "?"
	// TODO: verify improvements with: DELETE FROM resmon FORCE INDEX (idx_node_id_updated)
	query := fmt.Sprintf("DELETE FROM `resmon` WHERE `node_id` IN (%s) AND `updated` < ?", placeholders)
	args := make([]any, len(l)+1)
	for i, v := range l {
		args[i] = v
	}
	args[len(l)] = maxTime

	if count, err := oDb.execCountContext(ctx, query, args...); err != nil {
		return err
	} else if count > 0 {
		oDb.Metrics.ResourceStatusDelete.Add(float64(count))
	}
	return nil
}

func (r *DBResmonLog) IDx() string {
	return r.SvcID + "@" + r.NodeID + ":" + r.RID
}

func (r *DBInstanceResource) IDx() string {
	return r.SvcID + "@" + r.NodeID + ":" + r.RID
}

func (r *DBInstanceResource) SameAsLog(logStatus *DBResmonLog) bool {
	if logStatus == nil {
		return false
	}
	return r.Status == logStatus.Status && r.Log == logStatus.Log
}

func (r *DBInstanceResource) AsLog() *DBResmonLog {
	return &DBResmonLog{
		SvcID:   r.SvcID,
		NodeID:  r.NodeID,
		RID:     r.RID,
		Status:  r.Status,
		Log:     r.Log,
		BeginAt: r.Changed,
	}
}
