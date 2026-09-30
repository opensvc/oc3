package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// GetNetworks lists the declared networks, by name then address. Every
// authenticated user may read them, as in the historical collector.
func (oDb *DB) GetNetworks(ctx context.Context, p ListParams) ([]map[string]any, error) {
	return oDb.listQuery(ctx, "getNetworks", "networks", nil, nil, "networks.name, networks.network", p)
}

// NetworkRange is what a segment needs to know of its parent network: its
// usable range and the team responsible for it.
type NetworkRange struct {
	Network         string
	Netmask         int
	Begin           string
	End             string
	TeamResponsible string
}

// GetNetworkRange returns the range and responsible team of a declared network.
func (oDb *DB) GetNetworkRange(ctx context.Context, id int) (NetworkRange, bool, error) {
	var (
		r       NetworkRange
		netmask sql.NullInt64
	)
	err := oDb.DB.QueryRowContext(ctx,
		`SELECT COALESCE(network, ''), netmask, COALESCE(begin, ''), COALESCE(end, ''),
		COALESCE(team_responsible, '') FROM networks WHERE id = ?`, id).
		Scan(&r.Network, &netmask, &r.Begin, &r.End, &r.TeamResponsible)
	if errors.Is(err, sql.ErrNoRows) {
		return r, false, nil
	}
	if err != nil {
		return r, false, fmt.Errorf("GetNetworkRange: %w", err)
	}
	r.Netmask = int(netmask.Int64)
	return r, true, nil
}

// NetworkSegmentOverlapping returns the id of a segment of the network whose
// range shares at least one address with begin–end, if any.
func (oDb *DB) NetworkSegmentOverlapping(ctx context.Context, netID int, begin, end string) (int, bool, error) {
	var id int
	err := oDb.DB.QueryRowContext(ctx,
		`SELECT id FROM network_segments WHERE net_id = ?
		AND INET_ATON(seg_begin) <= INET_ATON(?) AND INET_ATON(seg_end) >= INET_ATON(?) LIMIT 1`,
		netID, end, begin).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("NetworkSegmentOverlapping: %w", err)
	}
	return id, true, nil
}

// InsertNetworkSegment creates a segment of a network and, when groupID is set,
// makes that group responsible for it. It returns the segment id.
func (oDb *DB) InsertNetworkSegment(ctx context.Context, netID int, segType, begin, end string, groupID *int64) (int, error) {
	res, err := oDb.ExecContext(ctx,
		"INSERT INTO network_segments (net_id, seg_type, seg_begin, seg_end) VALUES (?, ?, ?, ?)",
		netID, segType, begin, end)
	if err != nil {
		return 0, fmt.Errorf("InsertNetworkSegment: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("InsertNetworkSegment: %w", err)
	}
	oDb.SetChange("network_segments")
	if groupID != nil {
		if _, err := oDb.ExecContext(ctx,
			"INSERT INTO network_segment_responsibles (seg_id, group_id) VALUES (?, ?)", id, *groupID); err != nil {
			return int(id), fmt.Errorf("InsertNetworkSegment: responsible: %w", err)
		}
		oDb.SetChange("network_segment_responsibles")
	}
	return int(id), nil
}

// GetNetworkSegmentRow returns a network segment.
func (oDb *DB) GetNetworkSegmentRow(ctx context.Context, id int) (map[string]any, error) {
	var (
		rowID, netID        int
		segType, begin, end string
	)
	err := oDb.DB.QueryRowContext(ctx,
		`SELECT id, COALESCE(net_id, 0), COALESCE(seg_type, ''), COALESCE(seg_begin, ''), COALESCE(seg_end, '')
		FROM network_segments WHERE id = ?`, id).Scan(&rowID, &netID, &segType, &begin, &end)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("GetNetworkSegmentRow: %w", err)
	}
	return map[string]any{"id": rowID, "net_id": netID, "seg_type": segType, "seg_begin": begin, "seg_end": end}, nil
}
