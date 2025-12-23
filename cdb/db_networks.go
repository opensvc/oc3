package cdb

import (
	"context"
	"fmt"
	"time"
)

type (
	VNetwork struct {
		NodeID   string
		NodeEnv  string
		Addr     string
		NodeMask int
		NetMask  *int
		NetName  *string
	}
)

func (oDb *DB) NetworksWithWrongMask(ctx context.Context) (l []VNetwork, err error) {
	defer logDuration("NetworksWithWrongMask", time.Now())
	const (
		query = `SELECT
                    node_id,
                    node_env,
                    addr,
                    mask,
                    net_netmask,
                    net_name
                  FROM v_nodenetworks
                  WHERE
                    mask < net_netmask AND
                    mask != "" AND
                    addr_updated>DATE_SUB(NOW(), INTERVAL 2 DAY)`
	)
	rows, err := oDb.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("NetworksWithWrongMask query rows: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var o VNetwork
		err = rows.Scan(&o.NodeID, &o.NodeEnv, &o.Addr, &o.NodeMask, &o.NetMask, &o.NetName)
		if err != nil {
			err = fmt.Errorf("NetworksWithWrongMask scan: %w", err)
		}
		l = append(l, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("NetworksWithWrongMask query rows: %w", err)
	}
	return l, err
}
