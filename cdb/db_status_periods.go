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

// StatusAck is the justification of a period of a service: why it was not
// available, by whom and when, and whether it still counts in the availability.
type StatusAck struct {
	Begin   string
	End     string
	Comment string
	Account bool
	By      string
	On      string
}

// ServiceStatusAcks returns the justified periods of a service ending in the
// last days, oldest first.
func (oDb *DB) ServiceStatusAcks(ctx context.Context, svcID string, days int) ([]StatusAck, error) {
	query := `SELECT DATE_FORMAT(mon_begin, ` + periodDateFormat + `), DATE_FORMAT(mon_end, ` + periodDateFormat + `),
			mon_comment, mon_account, mon_acked_by, DATE_FORMAT(mon_acked_on, ` + periodDateFormat + `)
		FROM svcmon_log_ack
		WHERE svc_id = ? AND mon_end >= NOW() - INTERVAL ? DAY
		ORDER BY mon_begin`
	rows, err := oDb.DB.QueryContext(ctx, query, svcID, days)
	if err != nil {
		return nil, fmt.Errorf("serviceStatusAcks: %w", err)
	}
	defer func() { _ = rows.Close() }()
	acks := []StatusAck{}
	for rows.Next() {
		var a StatusAck
		if err := rows.Scan(&a.Begin, &a.End, &a.Comment, &a.Account, &a.By, &a.On); err != nil {
			return nil, fmt.Errorf("serviceStatusAcks: %w", err)
		}
		acks = append(acks, a)
	}
	return acks, rows.Err()
}

// SetServiceStatusAck justifies a period of a service, or changes its
// justification, the period being named by its bounds.
func (oDb *DB) SetServiceStatusAck(ctx context.Context, svcID, begin, end, comment string, account bool, by string) error {
	const query = `INSERT INTO svcmon_log_ack (svc_id, mon_begin, mon_end, mon_comment, mon_account, mon_acked_by, mon_acked_on)
		VALUES (?, ?, ?, ?, ?, ?, NOW())
		ON DUPLICATE KEY UPDATE mon_comment = VALUES(mon_comment), mon_account = VALUES(mon_account),
			mon_acked_by = VALUES(mon_acked_by), mon_acked_on = NOW()`
	if _, err := oDb.DB.ExecContext(ctx, query, svcID, begin, end, comment, account, by); err != nil {
		return fmt.Errorf("setServiceStatusAck: %w", err)
	}
	oDb.SetChange("svcmon_log_ack")
	return nil
}

// DeleteServiceStatusAck removes the justification of a period of a service;
// found is false when the period was not justified.
func (oDb *DB) DeleteServiceStatusAck(ctx context.Context, svcID, begin, end string) (bool, error) {
	result, err := oDb.DB.ExecContext(ctx,
		"DELETE FROM svcmon_log_ack WHERE svc_id = ? AND mon_begin = ? AND mon_end = ?", svcID, begin, end)
	if err != nil {
		return false, fmt.Errorf("deleteServiceStatusAck: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("deleteServiceStatusAck: %w", err)
	}
	if n > 0 {
		oDb.SetChange("svcmon_log_ack")
	}
	return n > 0, nil
}
