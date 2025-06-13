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
	// DBHeartbeat struct is for heartbeat table, it describes the heartbeat of a node.
	//
	// CREATE TABLE `hbmon` (
	//  `id` bigint(20) NOT NULL AUTO_INCREMENT,
	//  `cluster_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
	//  `node_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
	//  `peer_node_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
	//  `driver` varchar(32) DEFAULT '',
	//  `name` varchar(64) DEFAULT '',
	//  `desc` varchar(64) DEFAULT '',
	//  `state` enum('running','stopped','failed','unknown') DEFAULT '',
	//  `beating` tinyint(1) DEFAULT 0,
	// `last_beating` datetime NOT NULL,
	// `updated` timestamp NOT NULL DEFAULT current_timestamp(),
	//  PRIMARY KEY (`id`),
	//	KEY `k_cluster_id` (`cluster_id`),
	//  UNIQUE KEY `node_id` (`node_id`,`peer_node_id`,`name`)
	//) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci |
	DBHeartbeat struct {
		ID        int64
		clusterID string
		nodeID    string
		driver    string
		name      string
		desc      string
		state     string

		// peerNodeID when defined, it means the heartbeat is from a peer node,
		// so `beating` and `lastBeating`
		peerNodeID string
		// beating: 0: n/a, 1: beating, 2: not beating
		beating     int8
		lastBeating time.Time

		updated string
	}
)

func (oDb *opensvcDB) hbByClusterID(ctx context.Context, clusterID string) ([]DBHeartbeat, error) {
	const (
		query = "SELECT `id`, `cluster_id`, `node_id`, `peer_node_id`, `driver`, `name`, `desc`, `state`, `beating`, `updated`" +
			" FROM `hbmon`" +
			" WHERE `cluster_id` = ?"
	)
	var (
		l []DBHeartbeat
	)
	rows, err := oDb.db.QueryContext(ctx, query, clusterID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var ()
	for rows.Next() {
		var hb DBHeartbeat
		if err = rows.Scan(&hb.ID, &hb.clusterID, &hb.nodeID, &hb.peerNodeID, &hb.driver, &hb.name, &hb.desc, &hb.state, &hb.beating, &hb.updated); err != nil {
			return nil, err
		}
		l = append(l, hb)
	}
	return l, rows.Err()
}

func (oDb *opensvcDB) hbUpdate(ctx context.Context, hb DBHeartbeat) error {
	defer logDuration("hbUpdate cluster id:"+hb.clusterID+" "+hb.driver, time.Now())
	const (
		qUpdate = "" +
			"INSERT INTO `hbmon` (`cluster_id`, `node_id`, `peer_node_id`, `driver`, `name`, `desc`, `state`, `beating`, `last_beating`, `updated`)" +
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())" +
			"ON DUPLICATE KEY UPDATE" +
			" `cluster_id` = VALUES(`cluster_id`),  `driver` = VALUES(`driver`), `desc` = VALUES(`desc`), `state` = VALUES(`state`), `beating`= VALUES(`beating`),  `last_beating`= VALUES(`last_beating`), `updated`= VALUES(`updated`)"
	)
	_, err := oDb.db.ExecContext(ctx, qUpdate, hb.clusterID, hb.nodeID, hb.peerNodeID, hb.driver, hb.name, hb.desc, hb.state, hb.beating, hb.lastBeating)
	if err != nil {
		return err
	}
	oDb.tableChange("hbmon")
	return nil
}

func (oDb *opensvcDB) hbDeleteOutDatedByClusterID(ctx context.Context, clusterID string, maxTime time.Time) error {
	defer logDuration("hbDeleteOutDated cluster id:"+clusterID, time.Now())
	const (
		query = "" +
			"DELETE FROM `hbmon` WHERE `cluster_id` = ? AND `updated` < ?"
	)
	_, err := oDb.db.ExecContext(ctx, query, clusterID, maxTime)
	result, err := oDb.db.ExecContext(ctx, query, clusterID, maxTime)
	if err != nil {
		return fmt.Errorf("hbDeleteOutDatedByClusterID: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("hbDeleteOutDatedByClusterID count affected: %w", err)
	} else if affected > 0 {
		slog.Debug(fmt.Sprintf("hbDeleteOutDatedByClusterID %s %d", clusterID, affected))
		oDb.tableChange("hbmon")
	}
	return nil
}

func (oDb *opensvcDB) hbLogUpdate(ctx context.Context, hb DBHeartbeat) error {
	defer logDuration("hbLogUpdate "+hb.nodeID+":"+hb.peerNodeID+":"+hb.name, time.Now())
	// CREATE TABLE `hbmon_log` (
	//  `id` bigint(20) NOT NULL AUTO_INCREMENT,
	//  `cluster_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
	//  `node_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
	//  `peer_node_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
	//  `name` varchar(64) DEFAULT '',
	//  `desc` varchar(64) DEFAULT '',
	//  `state` enum('running','stopped','failed','unknown', '') DEFAULT '',
	//  `beating` tinyint(1) DEFAULT 0,
	//  `begin` datetime NOT NULL,
	//  `end` datetime NOT NULL,
	//  `updated` timestamp NOT NULL DEFAULT current_timestamp(),
	//  PRIMARY KEY (`id`),
	//  KEY `k_cluster_id` (`cluster_id`),
	//  UNIQUE KEY `k_node_peer_name_begin_end` (`node_id`,`peer_node_id`,`name`,`begin`, `end`)
	//) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci
	//
	//CREATE TABLE `hbmon_log_last` (
	//  `id` bigint(20) NOT NULL AUTO_INCREMENT,
	//  `cluster_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
	//  `node_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
	//  `peer_node_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
	//  `name` varchar(64) DEFAULT '',
	//  `state` enum('running','stopped','failed','unknown', '') DEFAULT '',
	//  `beating` tinyint(1) DEFAULT 0,
	//  `begin` datetime NOT NULL,
	//  `end` datetime NOT NULL,
	//  `updated` timestamp NOT NULL DEFAULT current_timestamp(),
	//  PRIMARY KEY (`id`),
	//  KEY `k_cluster_id` (`cluster_id`),
	//  UNIQUE KEY `k_node_peer_name` (`node_id`,`peer_node_id`,`name`)
	//) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci
	const (
		queryGetLogLast = "" +
			"SELECT `begin`, `id`, `state`, `beating` FROM `hbmon_log_last`" +
			" WHERE `node_id` = ? AND `peer_node_id` = ? AND `name` = ?"

		querySetLogLast = "" +
			"INSERT INTO `hbmon_log_last` (`cluster_id`, `node_id`, `peer_node_id`, `name`, `state`, `beating`, `begin`, `end`) " +
			" VALUES (?, ?, ?, ?, ?, ?, NOW(), NOW())" +
			" ON DUPLICATE KEY UPDATE " +
			"  `cluster_id`= VALUES(`cluster_id`), `state` = VALUES(`state`), `beating` = VALUES(`beating`), `end` = VALUES(`begin`), `end` = VALUES(`end`)"

		queryExtendIntervalOfCurrent = "" +
			"UPDATE `hbmon_log_last` SET `end` = NOW() " +
			" WHERE `node_id` = ? AND `peer_node_id` = ? AND `name` = ?"

		querySaveIntervalOfPreviousBeforeTransition = "" +
			"INSERT INTO `hbmon_log` (`cluster_id`, `node_id`, `peer_node_id`, `name`, " +
			" `state`, `beating`, `begin`, `end`) " +
			" VALUES (?, ?, ?, ?, ?, ?, ?, NOW())"
	)
	var (
		prev DBHeartbeat

		previousBegin time.Time
	)
	setLogLast := func() error {
		_, err := oDb.db.ExecContext(ctx, querySetLogLast, hb.clusterID, hb.nodeID, hb.peerNodeID, hb.name, hb.state, hb.beating)
		if err != nil {
			return fmt.Errorf("update hbmon_log_last: %w", err)
		}
		return nil
	}
	err := oDb.db.QueryRowContext(ctx, queryGetLogLast, hb.nodeID, hb.peerNodeID, hb.name).
		Scan(&previousBegin, &prev.ID, &prev.state, &prev.beating)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// set initial status, log value
		defer oDb.tableChange("hbmon_log")
		return setLogLast()
	case err != nil:
		return fmt.Errorf("get hbmon_log_last: %w", err)
	default:
		defer oDb.tableChange("hbmon_log")
		if hb.state == prev.state && hb.beating == prev.beating {
			// no change, extend last interval
			if _, err := oDb.db.ExecContext(ctx, queryExtendIntervalOfCurrent, hb.nodeID, hb.peerNodeID, hb.name); err != nil {
				return fmt.Errorf("extend hbmon_log_last: %w", err)
			}
			return nil
		} else {
			// the state or beating value will change, save interval of prev, log value before change
			_, err := oDb.db.ExecContext(ctx, querySaveIntervalOfPreviousBeforeTransition, hb.clusterID, hb.nodeID,
				hb.peerNodeID, hb.name, prev.state, prev.beating, previousBegin)
			if err != nil {
				return fmt.Errorf("save hbmon_log transition: %w", err)
			}
			// reset previousBegin and end interval for new hbmon log
			return setLogLast()
		}
	}
}
