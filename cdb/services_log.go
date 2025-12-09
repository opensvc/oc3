package cdb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type (
	ServiceLogUpdate struct {
		SvcID uuid.UUID

		// AvailStatus is the new value in "services"
		AvailStatus string

		// Begin value is:
		// * Now() if set from the scrub task
		// * the last received status time in other cases
		Begin time.Time

		// End value is:
		// * the last received status time, in any case
		//
		// It will land in the svc_end col of the new services_log line
		End time.Time
	}
)

func UpdateServicesLog(ctx context.Context, db DBOperater, entries ...ServiceLogUpdate) error {
	if len(entries) == 0 {
		return nil
	}

	svcIDs := make([]any, len(entries))
	ends := make(map[uuid.UUID]time.Time)

	for i, entry := range entries {
		svcIDs[i] = entry.SvcID
		ends[entry.SvcID] = entry.End
	}

	sql := fmt.Sprintf("SELECT svc_id, svc_availstatus, svc_begin FROM services_log_last WHERE svc_id IN (%s)", Placeholders(len(svcIDs)))
	rows, err := db.QueryContext(ctx, sql, svcIDs...)
	if err != nil {
		return fmt.Errorf("services_log_last select: %s: %w", sql, err)
	}

	last := make(map[uuid.UUID]ServiceLogUpdate)
	for {
		ok := rows.Next()
		if !ok {
			break
		}
		var line ServiceLogUpdate
		err := rows.Scan(&line.SvcID, &line.AvailStatus, &line.Begin)
		if err != nil {
			return fmt.Errorf("services_log_last line scan: %w", err)
		}
		last[line.SvcID] = line
	}

	logLastArgs := make([]any, 0)
	logLastLines := make([]string, 0)
	logArgs := make([]any, 0)
	logLines := make([]string, 0)

	for _, entry := range entries {
		logLastLines = append(logLastLines, "(?, ?, ?, ?)")
		prevEntry, ok := last[entry.SvcID]
		if ok {
			logLastArgs = append(logLastArgs, entry.SvcID, entry.AvailStatus, entry.Begin, entry.End)
			if entry.AvailStatus != prevEntry.AvailStatus {
				logLines = append(logLines, "(?, ?, ?, ?)")
				logArgs = append(logArgs, prevEntry.SvcID, prevEntry.AvailStatus, prevEntry.Begin, entry.End)
			}
		} else {
			logLastArgs = append(logLastArgs, entry.SvcID, entry.AvailStatus, "NOW()", "NOW()")
		}
	}

	sql = "INSERT INTO services_log_last (svc_id, svc_availstatus, svc_begin, svc_end) VALUES %s ON DUPLICATE KEY UPDATE svc_id=VALUES(svc_id), svc_availstatus=VALUES(svc_availstatus), svc_end=VALUES(svc_end)"
	sql = fmt.Sprintf(sql, strings.Join(logLastLines, ","))
	_, err = db.ExecContext(ctx, sql, logLastArgs...)
	if err != nil {
		return fmt.Errorf("services_log_last update: %s: %w", sql, err)
	}
	if len(logLines) > 0 {
		sql = "INSERT INTO services_log (svc_id, svc_availstatus, svc_begin, svc_end) VALUES %s"
		sql = fmt.Sprintf(sql, strings.Join(logLines, ","))
		_, err = db.ExecContext(ctx, sql, logArgs...)
		if err != nil {
			return fmt.Errorf("services_log insert: %s: %w", sql, err)
		}
		//task.Session().SetChanges("services_log")
	}
	return nil

}
