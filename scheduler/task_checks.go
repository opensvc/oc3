package scheduler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-graphite/go-whisper"
	"github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/timeseries"
)

var TaskChecks = Task{
	name:    "checks",
	fn:      taskChecks,
	period:  time.Minute,
	timeout: 10 * time.Minute,
}

func taskChecks(ctx context.Context, task *Task) error {
	if task.Redis == nil {
		return fmt.Errorf("redis client is nil")
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		result, err := task.Redis.LPop(ctx, cachekeys.FeedChecksQ).Result()
		if err == redis.Nil {
			task.Infof("checks: queue is empty")
			break
		}
		if err != nil {
			return fmt.Errorf("lpop: %w", err)
		}

		task.Infof("checks: processing %s", result)
		err = processCheck(ctx, task, result)
		if err != nil {
			task.Errorf("process check %s: %s", result, err)
		}
	}
	return nil
}

func processCheck(ctx context.Context, task *Task, keyStr string) error {
	nodeID := keyStr
	if nodeID == "" {
		return fmt.Errorf("empty key")
	}

	valStr, err := task.Redis.HGet(ctx, cachekeys.FeedChecksH, keyStr).Result()
	if err != nil {
		return fmt.Errorf("hget %s: %w", keyStr, err)
	}

	var data []json.RawMessage
	if err := json.Unmarshal([]byte(valStr), &data); err != nil {
		return fmt.Errorf("unmarshal val: %w", err)
	}
	if len(data) != 2 {
		return fmt.Errorf("unexpected data length: %d", len(data))
	}

	var vars []string
	if err := json.Unmarshal(data[0], &vars); err != nil {
		return fmt.Errorf("unmarshal vars: %w", err)
	}
	var vals [][]any
	if err := json.Unmarshal(data[1], &vals); err != nil {
		return fmt.Errorf("unmarshal vals: %w", err)
	}

	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	// initial purge
	task.Infof("checks: purging live checks for node %s", nodeID)
	if err := odb.PurgeChecksLive(ctx, nodeID); err != nil {
		return err
	}

	if len(vals) == 0 {
		task.Infof("checks: no values to insert for node %s", nodeID)
		return odb.Commit()
	}

	// Column indices
	idxMap := make(map[string]int)
	for i, v := range vars {
		idxMap[v] = i
	}

	now := time.Now()

	idxUpdated, hasUpdated := idxMap["chk_updated"]
	idxNodeID, hasNodeID := idxMap["node_id"]

	if !hasUpdated {
		vars = append(vars, "chk_updated")
		for i := range vals {
			vals[i] = append(vals[i], now)
		}
	}
	if !hasNodeID {
		vars = append(vars, "node_id")
		for i := range vals {
			vals[i] = append(vals[i], nodeID)
		}
	}
	// refresh map
	for i, v := range vars {
		idxMap[v] = i
	}
	idxUpdated, hasUpdated = idxMap["chk_updated"]
	idxNodeID, hasNodeID = idxMap["node_id"]

	// Filter and update vals
	validVals := make([][]any, 0, len(vals))

	for i := range vals {
		row := vals[i]
		if hasUpdated && idxUpdated < len(row) {
			row[idxUpdated] = now
		}
		if hasNodeID && idxNodeID < len(row) {
			row[idxNodeID] = nodeID
		}
		validVals = append(validVals, row)
	}

	task.Infof("checks: inserting %d rows for node %s", len(validVals), nodeID)
	// Insert in batches
	batchSize := 100
	for i := 0; i < len(validVals); i += batchSize {
		end := i + batchSize
		if end > len(validVals) {
			end = len(validVals)
		}
		if err := odb.InsertChecksLive(ctx, vars, validVals[i:end]); err != nil {
			task.Errorf("insert checks batch %d-%d: %s", i, end, err)
			continue
		}
	}

	if err := odb.PurgeChecksLive(ctx, nodeID); err != nil {
		return err
	}

	if err := odb.Commit(); err != nil {
		return err
	}

	// Update timeseries
	rodb, err := task.DBXRO(ctx)
	if err != nil {
		return err
	}
	defer rodb.Rollback()

	checks, err := rodb.GetChecksLiveForNode(ctx, nodeID)
	if err != nil {
		return fmt.Errorf("get checks: %w", err)
	}

	for _, check := range checks {
		instance := check.ChkInstance
		if instance != "" {
			instance = base64.RawURLEncoding.EncodeToString([]byte(instance))
		}

		path, err := MakeWSPFilename("nodes/%s/checks/%s:%s:%s",
			nodeID,
			check.SvcID,
			check.ChkType,
			instance)

		if err != nil {
			task.Errorf("make wsp filename: %s", err)
			continue
		}

		if err := timeseries.Update(path, check.ChkValue, int(now.Unix()), timeseries.DefaultRetentions, whisper.Average, 0.5); err != nil {
			task.Errorf("timeseries update %s: %s", path, err)
		} else {
			task.Infof("timeseries updated for %s", path)
		}
	}

	// Update dashboard alerts
	if task.ev != nil {
		if err := task.ev.EventPublish("checks_change", map[string]any{"node_id": nodeID}); err != nil {
			task.Errorf("event publish: %s", err)
		}
	}

	return nil
}
