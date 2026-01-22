package cdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type (
	ActionQueueEntry struct {
		ID           int
		Status       string
		Command      string
		DateQueued   time.Time
		DateDequeued time.Time
		Ret          int
		Stdout       string
		Stderr       string
		ActionType   string
		UserId       *int
		FormId       *int
		ConnectTo    *string
		NodeId       string
		SvcId        string
		Fqdn         *string
		ListenerPort int
	}
)

func (oDb *DB) ActionQSetNow(ctx context.Context) error {
	const query = `SET @now := NOW();`
	_, err := oDb.DB.ExecContext(ctx, query)
	return err
}

func (oDb *DB) ActionQSetDequeuedToNow(ctx context.Context) error {
	const query = `UPDATE action_queue SET date_dequeued = @now WHERE status = 'W' AND date_dequeued=0;`
	_, err := oDb.DB.ExecContext(ctx, query)
	return err
}

func (oDb *DB) ActionQGetQueued(ctx context.Context) (lines []ActionQueueEntry, err error) {
	const query = `SELECT
    	a.id, a.command, a.action_type, a.connect_to, n.fqdn, n.listener_port, a.form_id 
		FROM action_queue a JOIN nodes n ON a.node_id=n.node_id WHERE a.status='W' AND a.date_dequeued=@now`

	var rows *sql.Rows

	rows, err = oDb.DB.QueryContext(ctx, query)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var line ActionQueueEntry
		if err = rows.Scan(&line.ID, &line.Command, &line.ActionType, &line.ConnectTo, &line.Fqdn, &line.ListenerPort, &line.FormId); err != nil {
			return
		}
		lines = append(lines, line)
	}
	err = rows.Err()
	return
}

func getPlaceholdersAndArgs(ids []int) (placeholders []string, args []any) {
	placeholders = make([]string, len(ids))
	args = make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}
	return
}

func (oDb *DB) ActionQSetUnreachable(ctx context.Context, unreachableIds []int) error {
	placeholders, args := getPlaceholdersAndArgs(unreachableIds)
	request := fmt.Sprintf(`update action_queue set
                       status='T',
                       date_dequeued=NOW(),
                       ret=1,
                       stdout="",
                       stderr="unreachable"
                     where id in (%s)`, strings.Join(placeholders, ","))

	_, err := oDb.DB.ExecContext(ctx, request, args...)
	return err
}

func (oDb *DB) ActionQSetInvalid(ctx context.Context, invalidIds []int) error {
	placeholders, args := getPlaceholdersAndArgs(invalidIds)
	request := fmt.Sprintf(`update action_queue set
                       status='T',
                       date_dequeued=NOW(),
                       ret=1,
                       stdout="",
                       stderr="invalid"
                     where id in (%s)`, strings.Join(placeholders, ","))

	_, err := oDb.DB.ExecContext(ctx, request, args...)
	return err
}

func (oDb *DB) ActionQSetNotified(ctx context.Context, notifiedIds []int) error {
	placeholders, args := getPlaceholdersAndArgs(notifiedIds)
	request := fmt.Sprintf(`update action_queue set status='N' where id in (%s) and status='W'`, strings.Join(placeholders, ","))
	_, err := oDb.DB.ExecContext(ctx, request, args...)
	return err
}

func (oDb *DB) ActionQSetQueued(ctx context.Context, queuedIds []int) error {
	placeholders, args := getPlaceholdersAndArgs(queuedIds)
	request := fmt.Sprintf(`update action_queue set status='Q' where id in (%s) and status='W'`, strings.Join(placeholders, ","))
	_, err := oDb.DB.ExecContext(ctx, request, args...)
	return err
}

func (oDb *DB) ActionQPurge(ctx context.Context) error {
	request := `delete from action_queue where date_dequeued<date_sub(now(), interval 1 day) and status in ('T', 'C')`
	_, err := oDb.DB.ExecContext(ctx, request)
	return err
}

func (oDb *DB) ActionQEventData(ctx context.Context) (map[string]any, error) {
	const query = `select
              (select count(id) from action_queue where status in ('Q', 'N', 'W', 'R', 'S')) as queued,
              (select count(id) from action_queue where ret!=0) as ko,
              (select count(id) from action_queue where ret=0 and status='T') as ok`
	row := oDb.DB.QueryRowContext(ctx, query)
	data := make(map[string]any)
	var queued, ko, ok int
	if err := row.Scan(&queued, &ko, &ok); err != nil {
		return data, err
	}
	data["queued"] = queued
	data["ko"] = ko
	data["ok"] = ok
	return data, nil
}

func (oDb *DB) ActionQSetRunning(ctx context.Context, runningIds []int) error {
	placeholders, args := getPlaceholdersAndArgs(runningIds)
	request := fmt.Sprintf(`update action_queue set status='R' where id in (%s)`, strings.Join(placeholders, ","))
	_, err := oDb.DB.ExecContext(ctx, request, args...)
	return err
}

func (oDb *DB) ActionQSetDone(ctx context.Context, id int, ret int, stdout, stderr string) error {
	request := `update action_queue set
					   status='T',
					   date_dequeued=NOW(),
					   ret=?,
					   stdout=?,
					   stderr=?
					 where id=?`
	_, err := oDb.DB.ExecContext(ctx, request, ret, stdout, stderr, id)
	return err
}

func (oDb *DB) ActionQGetActionQueueId(ctx context.Context, formId int) (int, error) {
	query := `select id from action_queue where form_id=?`
	row := oDb.DB.QueryRowContext(ctx, query, formId)
	var id int
	if err := row.Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

func (oDb *DB) ActionQCloseWorkflow(ctx context.Context, formId int) error {
	request := `update workflows set status="closed" where last_form_id=?`
	_, err := oDb.DB.ExecContext(ctx, request, formId)
	return err
}

func (oDb *DB) ActionQSetFormNextId(ctx context.Context, id int) error {
	request := `update forms_store set form_next_id=0 where id=?`
	_, err := oDb.DB.ExecContext(ctx, request, id)
	return err
}
