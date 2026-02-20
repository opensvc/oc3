package worker

import (
	"context"
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
		JobBase
		JobRedis
		JobDB

		nodename  string
		nodeID    string
		clusterID string
		data      map[string]any
	}
)

func newNodeDisk(nodename, nodeID, clusterID string) *jobFeedNodeDisk {
	return &jobFeedNodeDisk{
		JobBase: JobBase{
			name:   "nodeDisk",
			detail: "nodename: " + nodename + " nodeID: " + nodeID,
		},
		JobRedis: JobRedis{
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

func (d *jobFeedNodeDisk) getData(ctx context.Context) error {
	cmd := d.redis.HGet(ctx, cachekeys.FeedNodeDiskH, d.cachePendingIDX)
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
func (d *jobFeedNodeDisk) updateDB(ctx context.Context) error {
	var (
		// pathToObjectID is a map of an object path to object ID, to cache db results
		pathToObjectID = make(map[string]string)

		// appIDM is a map of objectID@nodeID to app ID, to cache appIDFromObjectOrNodeIDs results
		appIDM = make(map[string]int64)
	)
	data, ok := d.data["data"].([]any)
	if !ok {
		slog.Warn("unsupported node disk data format")
		return nil
	}
	nodeID := d.nodeID
	now := d.now

	for i := range data {
		line, ok := data[i].(map[string]any)
		if !ok {
			slog.Warn("unsupported disk entry format")
			return nil
		}
		line["model"] = strings.Trim(line["model"].(string), "'")
		objectPath := strings.Trim(line["object_path"].(string), "'")
		diskID := strings.Trim(line["id"].(string), "'")
		if len(diskID) == 17 && diskID[0] == '2' {
			// fix naa-16
			diskID = diskID[1:]
		} else if strings.HasPrefix(diskID, d.nodename+".") {
			diskID = strings.Replace(diskID, d.nodename+".", d.nodeID+".", 1)
		}

		if line["model"] == "OPEN-V" {
			// update the `diskinfo`.`disk_id` for hds disks
			// TODO: add test with hds data
			if len(diskID) < 30 {
				return fmt.Errorf("refuse too short diskid len for OPEN-V disk: %d for %s", len(diskID), diskID)
			}
			devID := strings.ToUpper(diskID[26:28] + ":" + diskID[28:30] + ":" + diskID[30:])
			portnamePrefix := "50" + devID[2:12] + `%`
			if newDiskID, err := d.oDb.DiskIDFromDiskinfoWithDevIDAndTargetID(ctx, devID, portnamePrefix, diskID); err != nil {
				return fmt.Errorf("search diskinfo on OPEN-V disk with diskID %s: %w", diskID, err)
			} else if newDiskID != "" {
				if err := d.oDb.UpdateDiskinfoDiskID(ctx, diskID, newDiskID); err != nil {
					return fmt.Errorf("UpdateDiskinfoDiskID on OPEN-V disk: %w", err)
				}
			}
		}

		diskL, err := d.oDb.DiskinfoByDiskID(ctx, diskID)
		if err != nil {
			return fmt.Errorf("DiskinfoByDiskID: %w", err)
		}

		if len(diskL) > 0 {
			disk0 := diskL[0]
			// TODO: ensure check arrayID == "NULL" is still valid
			if disk0.ArrayID == nodeID || disk0.ArrayID == "" || disk0.ArrayID == "NULL" {
				// diskinfo registered as a stub for a local disk
				line["local"] = "T"
				if len(diskL) == 1 {
					if changed, err := d.oDb.UpdateDiskinfoArrayID(ctx, diskID, nodeID); err != nil {
						return fmt.Errorf("UpdateDiskinfoArrayID: %w", err)
					} else if changed {
						d.oDb.SetChange("diskinfo")
					}
				}
			} else {
				// diskinfo registered by a array parser or an hv pushdisks
				line["local"] = "F"
			}
		}
		if strings.HasPrefix(diskID, d.nodeID+".") && len(diskL) == 0 {
			line["local"] = "T"
			devID := strings.TrimPrefix(diskID, d.nodeID+".")
			if changed, err := d.oDb.UpdateDiskinfoArrayAndDevIDsAndSize(ctx, diskID, nodeID, devID, int32(line["size"].(float64))); err != nil {
				return fmt.Errorf("updateDiskinfoArrayAndDevIDsAndSize: %w", err)
			} else if changed {
				d.oDb.SetChange("diskinfo")
			}
		} else if len(diskL) == 0 {
			line["local"] = "F"
			if changed, err := d.oDb.UpdateDiskinfoForDiskSize(ctx, diskID, int32(line["size"].(float64))); err != nil {
				return fmt.Errorf("updateDiskinfoForDiskSize: %w", err)
			} else if changed {
				d.oDb.SetChange("diskinfo")
			}

			if changed, err := d.oDb.UpdateDiskinfoSetMissingArrayID(ctx, diskID, nodeID); err != nil {
				return fmt.Errorf("updateDiskinfoSetMissingArrayID: %w", err)
			} else if changed {
				d.oDb.SetChange("diskinfo")
			}
		}

		line["id"] = diskID
		line["node_id"] = nodeID
		line["updated"] = now

		// defines prepare line["svc_id"]
		if objectPath != "" {
			if objectID, ok := pathToObjectID[objectPath]; ok {
				line["svc_id"] = objectID
			} else if created, objectID, err := d.oDb.ObjectIDFindOrCreate(ctx, objectPath, d.clusterID); err != nil {
				return fmt.Errorf("objectIDFindOrCreate: %w", err)
			} else {
				if created {
					// TODO: add metrics
					log.Debug(fmt.Sprintf("jobFeedNodeDisk will create service %s@%s with new svc_id: %s", objectPath, d.clusterID, objectID))
				}
				line["svc_id"] = objectID
				if objectID != "" {
					pathToObjectID[objectPath] = objectID
				}
			}
		} else {
			line["svc_id"] = ""
		}

		// Assigns line["app_id"] with appID, or nil if appID is not detected
		// line["app_id"] must be defined (mapping doesn't support Optional when its data
		// is []any).
		objectID := line["svc_id"].(string)
		if appID, ok := appIDM[objectID+"@"+nodeID]; ok {
			if appID != 0 {
				line["app_id"] = appID
			} else {
				line["app_id"] = nil
			}
		} else if appID, ok, err := d.oDb.AppIDFromObjectOrNodeIDs(ctx, nodeID, objectID); err != nil {
			return fmt.Errorf("appIDFromObjectOrNodeIDs: %w", err)
		} else if !ok {
			appIDM[objectID+"@"+nodeID] = 0
			line["app_id"] = nil
		} else {
			appIDM[objectID+"@"+nodeID] = appID
			if appID != 0 {
				line["app_id"] = appID
			} else {
				line["app_id"] = nil
			}
		}

		data[i] = line
	}

	request := mariadb.InsertOrUpdate{
		Table: "svcdisks",
		Mappings: mariadb.Mappings{
			mariadb.Mapping{To: "disk_id", From: "id"},
			mariadb.Mapping{To: "node_id"},
			mariadb.Mapping{To: "svc_id"},
			mariadb.Mapping{To: "disk_vendor", From: "vendor"},
			mariadb.Mapping{To: "disk_model", From: "model"},
			mariadb.Mapping{To: "disk_dg", From: "dg"},
			mariadb.Mapping{To: "disk_region", From: "region", Modify: mariadb.ModifyToString},
			mariadb.Mapping{To: "disk_size", From: "size"},
			mariadb.Mapping{To: "disk_used", From: "used"},
			mariadb.Mapping{To: "disk_local", From: "local"},
			mariadb.Mapping{To: "app_id"},
			mariadb.Mapping{To: "disk_updated", From: "updated"},
		},
		Keys: []string{"disk_id", "svc_id", "node_id", "disk_dg"},
		Data: data,
	}
	if count, err := request.ExecContextAndCountRowsAffected(ctx, d.db); err != nil {
		return fmt.Errorf("updateDB insert: %w", err)
	} else if count > 0 {
		d.oDb.SetChange("svcdisks")
	}

	query := "DELETE FROM `svcdisks` WHERE `node_id` = ? AND `disk_updated` < ?"
	if count, err := d.oDb.ExecContextAndCountRowsAffected(ctx, query, nodeID, now); err != nil {
		return fmt.Errorf("query %s: %w", query, err)
	} else if count > 0 {
		d.oDb.SetChange("svcdisks")
	}

	// TODO: validate delete query
	query = "DELETE FROM `diskinfo` WHERE `disk_arrayid` = ? AND `disk_updated` < ?"
	if count, err := d.oDb.ExecContextAndCountRowsAffected(ctx, query, nodeID, now); err != nil {
		return fmt.Errorf("query %s: %w", query, err)
	} else if count > 0 {
		d.oDb.SetChange("diskinfo")
	}
	return nil
}
