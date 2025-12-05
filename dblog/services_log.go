package dblog

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type (
	ServiceLogUpdate struct {
		SvcID       uuid.UUID
		AvailStatus string
		Begin       time.Time
	}
)

func Placeholders(n int) string {
	return strings.TrimRight(strings.Repeat("?,", n), ",")
}

func UpdateServicesLog(db *sql.DB, entries ...ServiceLogUpdate) error {
	if len(entries) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	svcIDs := make([]any, len(entries))

	for i, entry := range entries {
		svcIDs[i] = entry.SvcID
	}
	sql := fmt.Sprintf("SELECT svc_id, svc_availstatus, svc_begin FROM services_log_last WHERE svc_id IN (%s)", Placeholders(len(svcIDs)))
	rows, err := tx.Query(sql, svcIDs...)
	if err != nil {
		return err
	}

	prev := make(map[uuid.UUID]ServiceLogUpdate)
	for {
		ok := rows.Next()
		if !ok {
			break
		}
		var (
			svcID       uuid.UUID
			availStatus string
			begin       time.Time
		)
		err := rows.Scan(&svcID, &availStatus, &begin)
		if err != nil {
			return err
		}
		prev[svcID] = ServiceLogUpdate{
			SvcID:       svcID,
			AvailStatus: availStatus,
			Begin:       begin,
		}
	}

	args := make([]any, 0)
	lines := make([]string, 0)
	logArgs := make([]any, 0)
	logLines := make([]string, 0)

	for _, entry := range entries {
		prevEntry, ok := prev[entry.SvcID]
		lines = append(lines, "(?, ?, ?)")
		if ok {
			args = append(args, entry.SvcID, entry.AvailStatus, prevEntry.Begin)
			if entry.AvailStatus != prevEntry.AvailStatus {
				logLines = append(logLines, "(?, ?, ?, ?)")
				logArgs = append(logArgs, prevEntry.SvcID, prevEntry.AvailStatus, prevEntry.Begin, "NOW()")
			}
		} else {
			args = append(args, entry.SvcID, entry.AvailStatus, "NOW()")
		}
	}

	sql = "INSERT INTO services_log_last (svc_id, svc_availstatus, svc_begin) VALUES %s ON DUPLICATE KEY UPDATE svc_id=VALUES(svc_id), svc_availstatus=VALUES(svc_availstatus), svc_begin=VALUES(svc_begin)"
	sql = fmt.Sprintf(sql, strings.Join(lines, ","))
	_, err = tx.Exec(sql, args...)
	if err != nil {
		return err
	}

	sql = "INSERT INTO services_log (svc_id, svc_availstatus, svc_begin, svc_end) VALUES %s"
	sql = fmt.Sprintf(sql, strings.Join(logLines, ","))
	_, err = tx.Exec(sql, logArgs...)
	if err != nil {
		return err
	}
	return nil

}
