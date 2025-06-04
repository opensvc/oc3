package worker

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/mariadb"
)

type (
	jobFeedNodeDisk struct {
		*BaseJob

		nodename  string
		nodeID    string
		clusterID string
		data      map[string]any
	}
)

func newNodeDisk(nodename, nodeID, clusterID string) *jobFeedNodeDisk {
	return &jobFeedNodeDisk{
		BaseJob: &BaseJob{
			name:   "nodeDisk",
			detail: "nodename: " + nodename + " nodeID: " + nodeID,

			cachePendingH:   cachekeys.FeedNodeDiskPendingH,
			cachePendingIDX: nodename + "@" + nodeID + "@" + clusterID,
		},
		clusterID: clusterID,
		nodeID:    nodeID,
		nodename:  nodename,
	}
}

func (d *jobFeedNodeDisk) Operations() []operation {
	return []operation{
		{desc: "nodeDisk/dropPending", do: d.dropPending},
		{desc: "nodeDisk/getData", do: d.getData},
		{desc: "nodeDisk/dbNow", do: d.dbNow},
		{desc: "nodeDisk/updateDB", do: d.updateDB},
		{desc: "nodeDisk/pushFromTableChanges", do: d.pushFromTableChanges},
	}
}

func (d *jobFeedNodeDisk) getData() error {
	cmd := d.redis.HGet(d.ctx, cachekeys.FeedNodeDiskH, d.cachePendingIDX)
	result, err := cmd.Result()
	switch err {
	case nil:
	case redis.Nil:
		return fmt.Errorf("HGET: no results")
	default:
		return fmt.Errorf("HGET: %w", err)
	}
	if err := json.Unmarshal([]byte(result), &d.data); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	return nil
}

// updateDB updates the database with the data from the feed.
//
//	{
//	   "data": [
//	     {
//	       "id": "vdb",
//	       "object_path": "",
//	       "size": 20480,
//	       "used": 20480,
//	       "vendor": "0x1af4",
//	       "model": "",
//	       "group": "",
//	       "nodename": "",
//	       "region": 0
//	     },
//	     {
//	       "id": "36589cfc00000047d94c2be911ef94b44",
//	       "object_path": "demodsk",
//	       "size": 128,
//	       "used": 128,
//	       "vendor": "TrueNAS ",
//	       "model": "iSCSI Disk",
//	       "group": "",
//	       "nodename": "",
//	       "region": 0
//	     }
//	   ]
//	 }
//
//			CREATE TABLE `svcdisks` (
//			   `id` int(11) NOT NULL AUTO_INCREMENT,
//			   `disk_id` varchar(120) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
//			   `disk_size` int(11) NOT NULL DEFAULT 0,
//			   `disk_vendor` varchar(60) DEFAULT NULL,
//			   `disk_model` varchar(60) DEFAULT NULL,
//			   `disk_dg` varchar(60) DEFAULT '',
//			   `disk_devid` varchar(60) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
//			   `disk_arrayid` varchar(60) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
//			   `disk_updated` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
//			   `disk_local` varchar(1) DEFAULT 'T',
//			   `disk_used` int(11) NOT NULL DEFAULT 0,
//			   `disk_region` varchar(32) DEFAULT '0',
//			   `node_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
//			   `app_id` int(11) DEFAULT NULL,
//			   `svc_id` char(36) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT '',
//			   PRIMARY KEY (`id`),
//			   UNIQUE KEY `uk_svcdisks_1` (`disk_id`,`svc_id`,`node_id`,`disk_dg`),
//			   KEY `idx1` (`disk_id`,`node_id`,`disk_dg`),
//			   KEY `k_node_id` (`node_id`),
//			   KEY `k_svc_id` (`svc_id`),
//			   KEY `k_svcdisks_1` (`svc_id`,`node_id`)
//			) ENGINE=InnoDB AUTO_INCREMENT=4641237 DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci
func (d *jobFeedNodeDisk) updateDB() error {
	data, ok := d.data["data"].([]any)
	if !ok {
		slog.Warn("unsupported node disk data format")
		return nil
	}
	nodeID := d.nodeID
	now := d.now
	pathToObjectID := make(map[string]string)
	for i, _ := range data {
		line, ok := data[i].(map[string]any)
		if !ok {
			slog.Warn("unsupported disk entry format")
			return nil
		}
		line["model"] = strings.Trim(line["model"].(string), "'")
		line["object_path"] = strings.Trim(line["object_path"].(string), "'")
		diskID := strings.Trim(line["id"].(string), "'")
		if len(diskID) == 17 && diskID[0] == '2' {
			// fix naa-16
			diskID = diskID[1:]
		} else if strings.HasPrefix(diskID, d.nodename+".") {
			diskID = strings.Replace(diskID, d.nodename+".", d.nodeID+".", 1)
		}
		if strings.HasPrefix(diskID, d.nodeID+".") {
			line["local"] = "T"
		} else {
			line["local"] = "F"
		}
		line["id"] = diskID
		line["node_id"] = nodeID
		line["updated"] = now
		if objectPath, ok := line["object_path"].(string); ok && objectPath != "" {
			if objectID, ok := pathToObjectID[objectPath]; ok {
				if objectID != "" {
					line["svc_id"] = objectID
				}
			} else if _, objectID, err := d.oDb.objectIDFindOrCreate(d.ctx, objectPath, d.clusterID); err != nil {
				return fmt.Errorf("objectIDFindOrCreate: %w", err)
			} else if objectID != "" {
				line["svc_id"] = objectID
				pathToObjectID[objectPath] = objectID
			}
		} else {
			line["svc_id"] = ""
		}
		data[i] = line
	}

	request := mariadb.InsertOrUpdate{
		Table: "svcdisks",
		Mappings: mariadb.Mappings{
			mariadb.Mapping{To: "disk_id", From: "id"},
			mariadb.Mapping{To: "node_id"},
			mariadb.Mapping{To: "svc_id"},
			mariadb.Mapping{To: "disk_vendor", From: "vendor", Optional: true},
			mariadb.Mapping{To: "disk_model", From: "model", Optional: true},
			mariadb.Mapping{To: "disk_dg", From: "dg", Optional: true},
			mariadb.Mapping{To: "disk_region", From: "region", Modify: mariadb.ModifyToString},
			mariadb.Mapping{To: "disk_size", From: "size"},
			mariadb.Mapping{To: "disk_used", From: "used"},
			mariadb.Mapping{To: "disk_local", From: "local"},
			mariadb.Mapping{To: "disk_updated", From: "updated"},
		},
		Keys: []string{"disk_id", "svc_id", "node_id", "disk_dg"},
		Data: data,
	}
	if affected, err := request.ExecContextAndCountRowsAffected(d.ctx, d.db); err != nil {
		return fmt.Errorf("updateDB insert: %w", err)
	} else if affected > 0 {
		d.oDb.tableChange("svcdisks")
	}

	query := "DELETE FROM `svcdisks` WHERE `node_id` = ? AND `disk_updated` < ?"
	if result, err := d.db.ExecContext(d.ctx, query, nodeID, now); err != nil {
		return fmt.Errorf("query %s: %w", query, err)
	} else if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("query %s count row affected: %w", query, err)
	} else if affected > 0 {
		d.oDb.tableChange("svcdisks")
	}

	// TODO: validate delete query
	query = "DELETE FROM `diskinfo` WHERE `disk_arrayid` = ? AND `disk_updated` < ?"
	if result, err := d.db.ExecContext(d.ctx, query, nodeID, now); err != nil {
		return fmt.Errorf("query %s: %w", query, err)
	} else if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("query %s count row affected: %w", query, err)
	} else if affected > 0 {
		d.oDb.tableChange("diskinfo")
	}
	return nil
}
