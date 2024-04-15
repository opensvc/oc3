package worker

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (oDb *opensvcDB) findClusterNodesWithNodenames(ctx context.Context, clusterID string, nodes []string) (dbNodes []*DBNode, err error) {
	defer logDuration("findClusterNodesWithNodenames", time.Now())
	if len(nodes) == 0 {
		err = fmt.Errorf("findClusterNodesWithNodenames: need nodes")
		return
	}
	var (
		rows *sql.Rows

		query = `SELECT nodename, node_id, node_env, app, hv, node_frozen,
			loc_country, loc_city, loc_addr, loc_building, loc_floor, loc_room, loc_rack, loc_zip,
		    enclosure, enclosureslot
		FROM nodes
		WHERE cluster_id = ? AND nodename IN (?`
	)
	args := []any{clusterID, nodes[0]}
	for i := 1; i < len(nodes); i++ {
		query += ", ?"
		args = append(args, nodes[i])
	}
	query += ")"

	rows, err = oDb.db.QueryContext(ctx, query, args...)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			nodename, nodeID, nodeEnv, app, hv, frozen, locCountry            sql.NullString
			locCity, locAddr, locBuilding, locFloor, locRoom, locRack, locZip sql.NullString
			enclosure, enclosureSlot                                          sql.NullString
		)
		err = rows.Scan(
			&nodename, &nodeID, &nodeEnv, &app, &hv, &frozen,
			&locCountry, &locCity, &locAddr, &locBuilding, &locFloor, &locRoom, &locRack, &locZip,
			&enclosure, &enclosureSlot)
		if err != nil {
			return
		}

		dbNodes = append(dbNodes, &DBNode{
			nodename:      nodename.String,
			frozen:        frozen.String,
			nodeID:        nodeID.String,
			clusterID:     clusterID,
			app:           app.String,
			nodeEnv:       nodeEnv.String,
			locAddr:       locAddr.String,
			locCountry:    locCountry.String,
			locCity:       locCity.String,
			locZip:        locZip.String,
			locBuilding:   locBuilding.String,
			locFloor:      locFloor.String,
			locRoom:       locRoom.String,
			locRack:       locRack.String,
			enclosure:     enclosure.String,
			enclosureSlot: enclosureSlot.String,
			hv:            hv.String,
		})
	}
	if err = rows.Err(); err != nil {
		return
	}
	return
}
