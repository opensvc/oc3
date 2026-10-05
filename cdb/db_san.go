package cdb

import (
	"context"
	"fmt"
)

// SANEndpoint is one pair of ends of a node's SAN wiring: a host bus adapter of
// the node and a target port zoned to it, with the array owning that port. Target
// and Array are empty for an adapter without a zoned target, Array alone for a
// target the collector knows no array for.
type SANEndpoint struct {
	HBA    string
	Target string
	Array  string
}

// SwitchPort is a port of a SAN switch. WWN names the switch the port belongs to
// (switches.sw_portname: the same for every port of a switch), Remote what is
// plugged on the other end: the port of an adapter or of an array, or, for an
// inter-switch link, the WWN of the other switch.
type SwitchPort struct {
	Name   string
	Fabric string
	WWN    string
	Remote string
	Type   string
	Index  int
	Speed  int
}

// NodeSANEndpoints returns the host bus adapters of a node with the target ports
// zoned to them. A caller who is not a manager only gets the adapters of the nodes
// its groups are responsible for, as for the adapters list.
func (oDb *DB) NodeSANEndpoints(ctx context.Context, nodeID string, groups []string, isManager bool) ([]SANEndpoint, error) {
	query := "SELECT node_hba.hba_id, COALESCE(stor_zone.tgt_id, ''), COALESCE(stor_array.array_name, '')" +
		" FROM node_hba" +
		" LEFT JOIN stor_zone ON stor_zone.hba_id = node_hba.hba_id" +
		" LEFT JOIN stor_array_tgtid ON stor_array_tgtid.array_tgtid = stor_zone.tgt_id" +
		" LEFT JOIN stor_array ON stor_array.id = stor_array_tgtid.array_id" +
		" WHERE node_hba.node_id = ?"
	args := []any{nodeID}
	if !isManager {
		cleanGroups := cleanGroups(groups)
		if len(cleanGroups) == 0 {
			return nil, nil
		}
		query += " AND node_hba.node_id IN (" +
			"SELECT n.node_id FROM nodes n" +
			" JOIN apps a ON n.app = a.app" +
			" JOIN apps_responsibles ar ON ar.app_id = a.id" +
			" JOIN auth_group ag ON ag.id = ar.group_id" +
			" WHERE ag.role IN (" + Placeholders(len(cleanGroups)) + ")" +
			")"
		for _, g := range cleanGroups {
			args = append(args, g)
		}
	}
	query += " ORDER BY node_hba.hba_id, stor_zone.tgt_id"

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("NodeSANEndpoints: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []SANEndpoint
	for rows.Next() {
		var e SANEndpoint
		if err := rows.Scan(&e.HBA, &e.Target, &e.Array); err != nil {
			return nil, fmt.Errorf("NodeSANEndpoints: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// SwitchPorts returns the connected ports of every SAN switch: the wiring a SAN
// path is followed through. A port with nothing plugged in leads nowhere and is
// left out.
func (oDb *DB) SwitchPorts(ctx context.Context) ([]SwitchPort, error) {
	const query = "SELECT sw_name, COALESCE(sw_fabric, ''), COALESCE(sw_portname, ''), sw_rportname," +
		" COALESCE(sw_porttype, ''), COALESCE(sw_index, 0), COALESCE(sw_portspeed, 0)" +
		" FROM switches" +
		" WHERE sw_rportname IS NOT NULL AND sw_rportname <> '' AND sw_portname IS NOT NULL AND sw_portname <> ''" +
		" ORDER BY sw_name, sw_index"
	rows, err := oDb.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("SwitchPorts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []SwitchPort
	for rows.Next() {
		var p SwitchPort
		if err := rows.Scan(&p.Name, &p.Fabric, &p.WWN, &p.Remote, &p.Type, &p.Index, &p.Speed); err != nil {
			return nil, fmt.Errorf("SwitchPorts: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
