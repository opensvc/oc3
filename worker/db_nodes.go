package worker

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
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

func (oDb *opensvcDB) updateContainerNodeFromParent(ctx context.Context, cName, cApp string, pn *DBNode) error {
	const queryUpdate = `UPDATE nodes
    	SET updated = NOW(),
    	    loc_addr = ?, loc_country = ?, loc_zip = ?, loc_city = ?, loc_building = ?,
    	    loc_floor = ?, loc_room = ?, loc_rack = ?, hv = ?, enclosure = ?, enclosureslot = ?
    	`
	const queryWhere1 = ` WHERE nodename = ? AND app in (?, ?)`

	result, err := oDb.db.ExecContext(ctx, queryUpdate+queryWhere1,
		pn.locAddr, pn.locCountry, pn.locZip, pn.locCity, pn.locBuilding,
		pn.locFloor, pn.locRoom, pn.locRack, pn.hv, pn.enclosure, pn.enclosureSlot,
		cName, pn.app, cApp)
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil {
		return err
	} else if count > 0 {
		oDb.tableChange("nodes")
		return nil
	} else {
		apps, err := oDb.responsibleAppsForNode(ctx, pn.nodeID)
		if err != nil {
			return err
		}
		if len(apps) == 0 {
			slog.Debug(fmt.Sprintf("findClusterNodesWithNodenames responsibleAppsForNode hostname %s on %s no apps",
				cName, pn.nodeID))
			return nil
		}
		var queryWhere2 = ` WHERE nodename = ? AND app in (?`
		var args = []any{
			pn.locAddr, pn.locCountry, pn.locZip, pn.locCity, pn.locBuilding,
			pn.locFloor, pn.locRoom, pn.locRack, pn.hv, pn.enclosure, pn.enclosureSlot,
			cName, apps[0]}
		for i := 1; i < len(apps); i++ {
			queryWhere2 += `, ?`
			args = append(args, apps[i])
		}
		queryWhere2 += `)`
		result, err := oDb.db.ExecContext(ctx, queryUpdate+queryWhere2, args...)
		if err != nil {
			return err
		}
		if count, err := result.RowsAffected(); err != nil {
			return err
		} else if count > 0 {
			oDb.tableChange("nodes")
			return nil
		}
	}
	return nil
}

func (oDb *opensvcDB) findClusterNodesFromNodeID(ctx context.Context, nodeID string) (dbNodes []*DBNode, err error) {
	defer logDuration("findClusterNodesFromNodeID", time.Now())
	if nodeID == "" {
		err = fmt.Errorf("findClusterNodesFromNodeID: called with empty node id")
		return
	}
	var (
		rows *sql.Rows

		query = `SELECT nodename, node_id, cluster_id, node_env, app, hv, node_frozen,
				loc_country, loc_city, loc_addr, loc_building, loc_floor, loc_room, loc_rack, loc_zip,
				enclosure, enclosureslot
			FROM nodes
			WHERE cluster_id IN (SELECT cluster_id FROM nodes WHERE node_id = ?)`
	)

	rows, err = oDb.db.QueryContext(ctx, query, nodeID)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			nodename, nodeID, clusterID, nodeEnv, app, hv, frozen, locCountry sql.NullString
			locCity, locAddr, locBuilding, locFloor, locRoom, locRack, locZip sql.NullString
			enclosure, enclosureSlot                                          sql.NullString
		)
		err = rows.Scan(
			&nodename, &nodeID, &clusterID, &nodeEnv, &app, &hv, &frozen,
			&locCountry, &locCity, &locAddr, &locBuilding, &locFloor, &locRoom, &locRack, &locZip,
			&enclosure, &enclosureSlot)
		if err != nil {
			return
		}

		dbNodes = append(dbNodes, &DBNode{
			nodename:      nodename.String,
			frozen:        frozen.String,
			nodeID:        nodeID.String,
			clusterID:     clusterID.String,
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
