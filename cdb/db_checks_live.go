package cdb

import (
	"context"
	"fmt"

	"strings"
	"time"
)

func (oDb *DB) PurgeChecksLive(ctx context.Context, nodeID string) error {
	defer logDuration("PurgeChecksLive", time.Now())
	query := `DELETE FROM checks_live WHERE node_id = ? AND chk_type NOT IN ("netdev_err", "save") AND chk_updated < DATE_SUB(NOW(), INTERVAL 20 SECOND)`
	if _, err := oDb.DB.ExecContext(ctx, query, nodeID); err != nil {
		return fmt.Errorf("PurgeChecksLive: %w", err)
	}
	return nil
}

func (oDb *DB) InsertChecksLive(ctx context.Context, vars []string, vals [][]any) error {
	defer logDuration("InsertChecksLive", time.Now())
	if len(vals) == 0 {
		return nil
	}

	var placeHolders []string
	var values []any

	placeHolder := "(" + strings.Repeat("?,", len(vars)-1) + "?)"

	for _, val := range vals {
		placeHolders = append(placeHolders, placeHolder)
		values = append(values, val...)
	}

	query := fmt.Sprintf("INSERT INTO checks_live (%s) VALUES %s", strings.Join(vars, ","), strings.Join(placeHolders, ","))

	if _, err := oDb.DB.ExecContext(ctx, query, values...); err != nil {
		return fmt.Errorf("InsertChecksLive: %w", err)
	}
	return nil
}

type CheckLive struct {
	NodeID      string
	SvcID       string
	ChkType     string
	ChkInstance string
	ChkValue    float64
}

func (oDb *DB) GetChecksLiveForNode(ctx context.Context, nodeID string) ([]CheckLive, error) {
	defer logDuration("GetChecksLiveForNode", time.Now())
	query := `SELECT node_id, svc_id, chk_type, chk_instance, chk_value FROM checks_live WHERE node_id = ?`
	rows, err := oDb.DB.QueryContext(ctx, query, nodeID)
	if err != nil {
		return nil, fmt.Errorf("GetChecksLiveForNode: %w", err)
	}
	defer rows.Close()

	var result []CheckLive
	for rows.Next() {
		var r CheckLive
		var svcId, chkInstance *string
		if err := rows.Scan(&r.NodeID, &svcId, &r.ChkType, &chkInstance, &r.ChkValue); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		if svcId != nil {
			r.SvcID = *svcId
		}
		if chkInstance != nil {
			r.ChkInstance = *chkInstance
		}
		result = append(result, r)
	}
	return result, nil
}
