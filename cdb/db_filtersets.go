package cdb

import (
	"context"
	"database/sql"
	"time"
)

type (
	Filterset struct {
		ID      uint64
		Name    string
		Author  string
		Updated time.Time
		Stats   bool
	}

	FiltersetIDsMap map[uint64]FiltersetIDs

	FiltersetIDs struct {
		NodeIDs string
		SvcIDs  string
	}
)

func (oDb *DB) ResolveFiltersets(ctx context.Context) (FiltersetIDsMap, error) {
	const query = `
		SELECT
		    fset_id,
		    GROUP_CONCAT(IF(obj_type = 'svc_id', CONCAT('"', obj_id, '"'), NULL)) AS svc_ids,
		    GROUP_CONCAT(IF(obj_type = 'node_id', CONCAT('"', obj_id, '"'), NULL)) AS node_ids
		FROM
		    fset_cache
		GROUP BY
		    fset_id;
		`
	m := make(FiltersetIDsMap)
	rows, err := oDb.DB.QueryContext(ctx, query)
	if err != nil {
		return m, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			id              uint64
			svcIDs, nodeIDs sql.NullString
		)
		if err = rows.Scan(&id, &svcIDs, &nodeIDs); err != nil {
			return m, err
		}
		e := FiltersetIDs{}
		if nodeIDs.Valid {
			e.NodeIDs = nodeIDs.String
		} else {
			e.NodeIDs = "NULL"
		}
		if svcIDs.Valid {
			e.SvcIDs = svcIDs.String
		} else {
			e.SvcIDs = "NULL"
		}
		m[id] = e
	}
	if err = rows.Err(); err != nil {
		return m, err
	}
	return m, nil
}

func (oDb *DB) GetStatsFiltersets(ctx context.Context) (fsets []Filterset, err error) {
	const query = `
                SELECT id, fset_name, fset_author, fset_updated, fset_stats
                FROM gen_filtersets
                WHERE fset_stats="T"`
	rows, err := oDb.DB.QueryContext(ctx, query)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var fset Filterset
		if err = rows.Scan(&fset.ID, &fset.Name, &fset.Author, &fset.Updated, &fset.Stats); err != nil {
			return
		}
		fsets = append(fsets, fset)
	}
	if err = rows.Err(); err != nil {
		return
	}
	return
}
