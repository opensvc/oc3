package cdb

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
		ClusterID string
		NodeID    string
		Driver    string
		Name      string
		Desc      string
		State     string

		// PeerNodeID when defined, it means the heartbeat is from a peer node,
		// so `beating` and `lastBeating`
		PeerNodeID string
		// Beating: 0: n/a, 1: Beating, 2: not Beating
		Beating     int8
		LastBeating time.Time

		Updated string
	}
)

func (oDb *DB) hbByClusterID(ctx context.Context, clusterID string) ([]DBHeartbeat, error) {
	const (
		query = "SELECT `id`, `cluster_id`, `node_id`, `peer_node_id`, `driver`, `name`, `desc`, `state`, `beating`, `updated`" +
			" FROM `hbmon`" +
			" WHERE `cluster_id` = ?"
	)
	var (
		l []DBHeartbeat
	)
	rows, err := oDb.DB.QueryContext(ctx, query, clusterID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var ()
	for rows.Next() {
		var hb DBHeartbeat
		if err = rows.Scan(&hb.ID, &hb.ClusterID, &hb.NodeID, &hb.PeerNodeID, &hb.Driver, &hb.Name, &hb.Desc, &hb.State, &hb.Beating, &hb.Updated); err != nil {
			return nil, err
		}
		l = append(l, hb)
	}
	return l, rows.Err()
}

func (oDb *DB) HBUpdate(ctx context.Context, hb DBHeartbeat) error {
	defer logDuration("HBUpdate cluster id:"+hb.ClusterID+" "+hb.Driver, time.Now())
	const (
		qUpdate = "" +
			"INSERT INTO `hbmon` (`cluster_id`, `node_id`, `peer_node_id`, `driver`, `name`, `desc`, `state`, `beating`, `last_beating`, `updated`)" +
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())" +
			"ON DUPLICATE KEY UPDATE" +
			" `cluster_id` = VALUES(`cluster_id`),  `driver` = VALUES(`driver`), `desc` = VALUES(`desc`), `state` = VALUES(`state`), `beating`= VALUES(`beating`),  `last_beating`= VALUES(`last_beating`), `updated`= VALUES(`updated`)"
	)
	_, err := oDb.DB.ExecContext(ctx, qUpdate, hb.ClusterID, hb.NodeID, hb.PeerNodeID, hb.Driver, hb.Name, hb.Desc, hb.State, hb.Beating, hb.LastBeating)
	if err != nil {
		return err
	}
	oDb.SetChange("hbmon")
	return nil
}

func (oDb *DB) HBDeleteOutDatedByClusterID(ctx context.Context, clusterID string, maxTime time.Time) error {
	defer logDuration("hbDeleteOutDated cluster id:"+clusterID, time.Now())
	const (
		query = "" +
			"DELETE FROM `hbmon` WHERE `cluster_id` = ? AND `updated` < ?"
	)
	_, err := oDb.DB.ExecContext(ctx, query, clusterID, maxTime)
	result, err := oDb.DB.ExecContext(ctx, query, clusterID, maxTime)
	if err != nil {
		return fmt.Errorf("hbDeleteOutDatedByClusterID: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("hbDeleteOutDatedByClusterID count affected: %w", err)
	} else if affected > 0 {
		slog.Debug(fmt.Sprintf("hbDeleteOutDatedByClusterID %s %d", clusterID, affected))
		oDb.SetChange("hbmon")
	}
	return nil
}

func (oDb *DB) HBLogUpdate(ctx context.Context, hb DBHeartbeat) error {
	defer logDuration("hbLogUpdate "+hb.NodeID+":"+hb.PeerNodeID+":"+hb.Name, time.Now())
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
			"  `cluster_id`= VALUES(`cluster_id`), `state` = VALUES(`state`), `beating` = VALUES(`beating`), `begin` = VALUES(`begin`), `end` = VALUES(`end`)"

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
		_, err := oDb.DB.ExecContext(ctx, querySetLogLast, hb.ClusterID, hb.NodeID, hb.PeerNodeID, hb.Name, hb.State, hb.Beating)
		if err != nil {
			return fmt.Errorf("update hbmon_log_last: %w", err)
		}
		return nil
	}

	err := oDb.DB.QueryRowContext(ctx, queryGetLogLast, hb.NodeID, hb.PeerNodeID, hb.Name).
		Scan(&previousBegin, &prev.ID, &prev.State, &prev.Beating)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// set initial status, log value
		defer oDb.SetChange("hbmon_log")
		return setLogLast()
	case err != nil:
		return fmt.Errorf("get hbmon_log_last: %w", err)
	default:
		defer oDb.SetChange("hbmon_log")
		if hb.State == prev.State && hb.Beating == prev.Beating {
			// no change, extend last interval
			if _, err := oDb.DB.ExecContext(ctx, queryExtendIntervalOfCurrent, hb.NodeID, hb.PeerNodeID, hb.Name); err != nil {
				return fmt.Errorf("extend hbmon_log_last: %w", err)
			}
			return nil
		} else {
			// the state or beating value will change, save interval of prev, log value before change
			_, err := oDb.DB.ExecContext(ctx, querySaveIntervalOfPreviousBeforeTransition, hb.ClusterID, hb.NodeID,
				hb.PeerNodeID, hb.Name, prev.State, prev.Beating, previousBegin)
			if err != nil {
				return fmt.Errorf("save hbmon_log transition %s -> %s name: %s state %s beating %d since %s: %w",
					hb.NodeID, hb.PeerNodeID, hb.Name, prev.State, prev.Beating, previousBegin,
					err)
			}
			// reset previousBegin and end interval for new hbmon log
			return setLogLast()
		}
	}
}
