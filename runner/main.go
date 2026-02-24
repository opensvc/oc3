package runner

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os/exec"
	"strings"
	"time"

	"github.com/spf13/viper"

	"github.com/opensvc/oc3/cdb"
)

type (
	ActionDaemon struct {
		DB *sql.DB
		Ev eventPublisher

		Ctx context.Context

		ids            []int
		nIds           []int
		invalidIds     []int
		unreachableIds []int
		runningIds     []int

		doneEntries []cmdSetDone
	}

	Worker struct {
		dispatchC chan cdb.ActionQueueEntry
		cmdC      chan any
		ctx       context.Context
	}

	cmdSetUnreachable struct {
		id int
	}

	cmdSetNotified struct {
		id int
	}

	cmdSetInvalid struct {
		id int
	}

	cmdSetRunning struct {
		id int
	}

	cmdSetDone struct {
		id     int
		ret    int
		stdout string
		stderr string
	}

	dedupLog struct {
		notified bool
	}

	eventPublisher interface {
		EventPublish(eventName string, data map[string]any) error
	}
)

const (
	DefaultNbWorkers           = 5
	DefaultNotificationTimeout = 5 * time.Second
	DefaultCommandTimeout      = 10 * time.Second
	DefaultPurgeTimeout        = 24 * time.Hour
)

func (d *ActionDaemon) Run() error {
	nbWorkers := getOptionInt("actiond.nb_workers", DefaultNbWorkers)
	purgeTimeout := getOptionDuration("actiond.purge_timeout", DefaultPurgeTimeout)
	odb := cdb.New(d.DB)
	dispatchC := make(chan cdb.ActionQueueEntry)
	cmdC := make(chan any)
	for i := 0; i < nbWorkers; i++ {
		w := Worker{
			dispatchC: dispatchC,
			cmdC:      cmdC,
			ctx:       d.Ctx,
		}
		go w.Run()
	}

	// pollTicker for polling waiting actions
	pollTicker := time.NewTicker(time.Second)
	defer pollTicker.Stop()

	// updateTicker for batching status updates
	updateTicker := time.NewTicker(5 * time.Second)
	defer updateTicker.Stop()

	purgeTicker := time.NewTicker(purgeTimeout)

	var nowErrorLogger, setDequeuedToNowErrorLogger, getQueuedErrorLogger dedupLog
	pollWaitingActions := func() {
		// define SQL @now as the time we start processing the queue
		err := odb.ActionQSetNow(d.Ctx)
		if err != nil {
			nowErrorLogger.warnf("set now: %s", err)
			return
		}
		nowErrorLogger.reset()

		// mark all waiting actions as dequeued at @now
		err = odb.ActionQSetDequeuedToNow(d.Ctx)
		if err != nil {
			setDequeuedToNowErrorLogger.warnf("set dequeued to now: %s", err)
			return
		}
		setDequeuedToNowErrorLogger.reset()

		// fetch all actions marked as dequeued at @now
		lines, err := odb.ActionQGetQueued(d.Ctx)
		if err != nil {
			getQueuedErrorLogger.warnf("get queued: %s", err)
			return
		}
		getQueuedErrorLogger.reset()

		// dispatch each action to a worker
		for _, line := range lines {
			dispatchC <- line
		}
	}
	for {
		select {
		case <-pollTicker.C:
			pollWaitingActions()
		case cmd := <-cmdC:
			switch cmd.(type) {
			case cmdSetUnreachable:
				c := cmd.(cmdSetUnreachable)
				d.unreachableIds = append(d.unreachableIds, c.id)
			case cmdSetNotified:
				c := cmd.(cmdSetNotified)
				d.nIds = append(d.nIds, c.id)
			case cmdSetInvalid:
				c := cmd.(cmdSetInvalid)
				d.invalidIds = append(d.invalidIds, c.id)
			case cmdSetRunning:
				c := cmd.(cmdSetRunning)
				d.runningIds = append(d.runningIds, c.id)
			case cmdSetDone:
				c := cmd.(cmdSetDone)
				d.doneEntries = append(d.doneEntries, c)
			}
		case <-updateTicker.C:
			if len(d.unreachableIds) > 0 {
				err := odb.ActionQSetUnreachable(d.Ctx, d.unreachableIds)
				if err != nil {
					slog.Warn(fmt.Sprintf("set unreachable: %s", err))
				} else {
					slog.Debug(fmt.Sprintf("set unreachable: %v", d.unreachableIds))
					d.unreachableIds = []int{}
				}
			}
			if len(d.invalidIds) > 0 {
				err := odb.ActionQSetInvalid(d.Ctx, d.invalidIds)
				if err != nil {
					slog.Warn(fmt.Sprintf("set invalid: %s", err))
				} else {
					slog.Debug(fmt.Sprintf("set invalid: %v", d.invalidIds))
					d.invalidIds = []int{}
				}
			}
			if len(d.nIds) > 0 {
				err := odb.ActionQSetNotified(d.Ctx, d.nIds)
				if err != nil {
					slog.Warn(fmt.Sprintf("set notified: %s", err))
				} else {
					slog.Debug(fmt.Sprintf("set notified: %v", d.nIds))
					d.nIds = []int{}
				}
			}
			if len(d.ids) > 0 {
				err := odb.ActionQSetQueued(d.Ctx, d.ids)
				if err != nil {
					slog.Warn(fmt.Sprintf("set queued: %s", err))
				} else {
					slog.Info(fmt.Sprintf("set queued: %v", d.ids))
					d.ids = []int{}
				}
			}
			if len(d.runningIds) > 0 {
				err := odb.ActionQSetRunning(d.Ctx, d.runningIds)
				if err != nil {
					slog.Warn(fmt.Sprintf("set running: %s", err))
				} else {
					slog.Debug(fmt.Sprintf("set running: %v", d.runningIds))
					d.runningIds = []int{}
				}
			}
			if len(d.doneEntries) > 0 {
				for _, entry := range d.doneEntries {
					err := odb.ActionQSetDone(d.Ctx, entry.id, entry.ret, entry.stdout, entry.stderr)
					if err != nil {
						slog.Warn(fmt.Sprintf("set done: %s", err))
					} else {
						slog.Debug(fmt.Sprintf("set done: id %d ret %d stdout %d stderr %d", entry.id, entry.ret, len(entry.stdout), len(entry.stderr)))
					}
				}
				d.doneEntries = []cmdSetDone{}
			}
			if len(d.ids) > 0 || len(d.nIds) > 0 || len(d.invalidIds) > 0 || len(d.unreachableIds) > 0 || len(d.runningIds) > 0 || len(d.doneEntries) > 0 {
				data, err := odb.ActionQEventData(d.Ctx)
				if err != nil {
					slog.Warn(fmt.Sprintf("get action queue event data: %s", err))
				}
				if err := odb.Session.NotifyTableChangeWithData(d.Ctx, "action_queue", data); err != nil {
					slog.Warn(fmt.Sprintf("notify changes: %s", err))
				}
			}
		case <-purgeTicker.C:
			if err := odb.ActionQPurge(d.Ctx); err != nil {
				slog.Warn(fmt.Sprintf("purge action queue: %s", err))
			} else {
				slog.Debug("purge action queue: done")
			}
		case <-d.Ctx.Done():
			return nil
		}
	}
}

func (w *Worker) Run() {
	for {
		select {
		case e := <-w.dispatchC:
			if err := w.work(e); err != nil {
				slog.Warn(err.Error())
			}
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *Worker) work(e cdb.ActionQueueEntry) error {
	if err := w.validateCommand(e.Command); err != nil {
		w.cmdC <- cmdSetInvalid{
			id: e.ID,
		}
		return fmt.Errorf("invalid command: %s", err)
	}
	switch e.ActionType {
	case "pull":
		w.workPull(e)
	case "push":
		w.workPush(e)
	default:
		return fmt.Errorf("unknown action type: %s", e.ActionType)
	}
	return nil
}

func notifyNode(nodename string, port int) error {
	addr := fmt.Sprintf("%s:%d", nodename, port)
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		return err
	}

	_, err = conn.Write([]byte("dequeue_actions"))
	return err
}

func (w *Worker) workPull(e cdb.ActionQueueEntry) {

	notifTimeout := getOptionDuration("actiond.notification_timeout", DefaultNotificationTimeout)

	ctx, cancel := context.WithTimeout(w.ctx, notifTimeout)
	defer cancel()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.cmdC <- cmdSetUnreachable{
				id: e.ID,
			}
			return
		case <-ticker.C:
			if e.ConnectTo == nil {
				continue
			}
			if err := notifyNode(*e.ConnectTo, e.ListenerPort); err != nil {
				continue
			}
			w.cmdC <- cmdSetNotified{
				id: e.ID,
			}
			return
		}
	}
}

func (w *Worker) workPush(e cdb.ActionQueueEntry) {
	w.cmdC <- cmdSetRunning{
		id: e.ID,
	}

	stdout, stderr, returnCode := executeCommand(w.ctx, e.Command)

	slog.Debug("command executed",
		"action_id", e.ID,
		"return_code", returnCode,
		"stdout_len", len(stdout),
		"stderr_len", len(stderr),
	)

	w.cmdC <- cmdSetDone{
		id:     e.ID,
		ret:    returnCode,
		stdout: strings.TrimSpace(stdout),
		stderr: strings.TrimSpace(stderr),
	}
}

func (w *Worker) validateCommand(cmd string) error {
	invalid := []string{
		"opensvc@localhost",
		"opensvc@localhost.localdomain",
	}

	for _, pattern := range invalid {
		if strings.Contains(cmd, pattern) {
			return fmt.Errorf("invalid command pattern: %s", pattern)
		}
	}

	if strings.TrimSpace(cmd) == "" {
		return fmt.Errorf("empty command")
	}

	return nil
}

func executeCommand(ctx context.Context, cmd string) (string, string, int) {
	cmdTimeout := getOptionDuration("actiond.command_timeout", DefaultCommandTimeout)

	ctx, cancel := context.WithTimeout(ctx, cmdTimeout)
	defer cancel()

	args := strings.Fields(cmd)

	command := exec.CommandContext(ctx, args[0], args[1:]...)

	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr

	err := command.Run()

	switch {
	case err == nil:
		return stdout.String(), stderr.String(), 0
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return stdout.String(), "command timeout", 1
	default:
		if exitErr, ok := err.(*exec.ExitError); ok {
			return stdout.String(), stderr.String(), exitErr.ExitCode()
		}
		return stdout.String(), err.Error(), 1
	}
}

func (d *dedupLog) warnf(format string, args ...any) {
	if !d.notified {
		slog.Warn(fmt.Sprintf(format, args...))
		d.notified = true
	}
}

func (d *dedupLog) reset() {
	d.notified = false
}

func getOptionInt(name string, defaultValue int) int {
	value := viper.GetInt(name)
	if value == 0 {
		return defaultValue
	}
	return value
}

func getOptionDuration(name string, defaultValue time.Duration) time.Duration {
	value := viper.GetDuration(name)
	if value == 0 {
		return defaultValue
	}
	return value
}
