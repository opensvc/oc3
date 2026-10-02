package cdb

import (
	"context"
	"fmt"
)

// StatusPeriod is a period during which a service, or an instance, kept the
// same statuses: Avail always, Overall for an instance.
type StatusPeriod struct {
	Avail   string
	Overall string
	Begin   string
	End     string
}

const periodDateFormat = "'%Y-%m-%d %H:%i:%s'"

// ServiceStatusPeriods returns the availability periods of a service ending in
// the last days: the closed ones of services_log, then the current one of
// services_log_last, oldest first.
func (oDb *DB) ServiceStatusPeriods(ctx context.Context, svcID string, days int) ([]StatusPeriod, error) {
	query := `SELECT svc_availstatus, DATE_FORMAT(svc_begin, ` + periodDateFormat + `), DATE_FORMAT(svc_end, ` + periodDateFormat + `)
		FROM (
			SELECT svc_availstatus, svc_begin, svc_end FROM services_log WHERE svc_id = ? AND svc_end >= NOW() - INTERVAL ? DAY
			UNION ALL
			SELECT svc_availstatus, svc_begin, svc_end FROM services_log_last WHERE svc_id = ?
		) t
		ORDER BY svc_begin`
	return oDb.statusPeriods(ctx, query, false, svcID, days, svcID)
}

// InstanceStatusPeriods returns the availability and overall status periods of
// an instance ending in the last days: the closed ones of svcmon_log, then the
// current one of svcmon_log_last, oldest first.
func (oDb *DB) InstanceStatusPeriods(ctx context.Context, svcID, nodeID string, days int) ([]StatusPeriod, error) {
	query := `SELECT mon_availstatus, mon_overallstatus, DATE_FORMAT(mon_begin, ` + periodDateFormat + `), DATE_FORMAT(mon_end, ` + periodDateFormat + `)
		FROM (
			SELECT mon_availstatus, mon_overallstatus, mon_begin, mon_end FROM svcmon_log
				WHERE svc_id = ? AND node_id = ? AND mon_end >= NOW() - INTERVAL ? DAY
			UNION ALL
			SELECT mon_availstatus, mon_overallstatus, mon_begin, mon_end FROM svcmon_log_last
				WHERE svc_id = ? AND node_id = ?
		) t
		ORDER BY mon_begin`
	return oDb.statusPeriods(ctx, query, true, svcID, nodeID, days, svcID, nodeID)
}

func (oDb *DB) statusPeriods(ctx context.Context, query string, withOverall bool, args ...any) ([]StatusPeriod, error) {
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("statusPeriods: %w", err)
	}
	defer func() { _ = rows.Close() }()
	periods := []StatusPeriod{}
	for rows.Next() {
		var p StatusPeriod
		var err error
		if withOverall {
			err = rows.Scan(&p.Avail, &p.Overall, &p.Begin, &p.End)
		} else {
			err = rows.Scan(&p.Avail, &p.Begin, &p.End)
		}
		if err != nil {
			return nil, fmt.Errorf("statusPeriods: %w", err)
		}
		periods = append(periods, p)
	}
	return periods, rows.Err()
}
