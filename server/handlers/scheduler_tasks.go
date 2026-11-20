package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetSchedulerTasks handles GET /scheduler/tasks.
func (a *Api) GetSchedulerTasks(c echo.Context, params server.GetSchedulerTasksParams) error {
	// The tasks, their runs and their errors tell the inside of the
	// collector: a Manager only, as changing them.
	if err := requireManager(c); err != nil {
		return err
	}
	return a.handleList(c, "GetSchedulerTasks", "scheduler_task", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetSchedulerTasks(ctx, p)
	})
}

// GetSchedulerTask handles GET /scheduler/tasks/{task_name}.
func (a *Api) GetSchedulerTask(c echo.Context, taskName server.InPathSchedulerTaskName, params server.GetSchedulerTaskParams) error {
	// The tasks, their runs and their errors tell the inside of the
	// collector: a Manager only, as changing them.
	if err := requireManager(c); err != nil {
		return err
	}
	return a.getSchedulerTask(c, "GetSchedulerTask", taskName, params.Props)
}

func (a *Api) getSchedulerTask(c echo.Context, name, taskName string, props *server.InQueryProps) error {
	return a.handleItem(c, name, "scheduler_task", "task_name", taskName, listEndpointParams{props: props},
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetSchedulerTask(ctx, taskName, p)
		})
}

// PutSchedulerTask handles PUT /scheduler/tasks/{task_name}: disables or
// enables a task.
func (a *Api) PutSchedulerTask(c echo.Context, taskName server.InPathSchedulerTaskName) error {
	log := echolog.GetLogHandler(c, "PutSchedulerTask")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if err := requireManager(c); err != nil {
		return err
	}
	var body server.PutSchedulerTaskJSONRequestBody
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	parent, found, err := a.ODB.SchedulerTaskParent(ctx, taskName)
	switch {
	case err != nil:
		log.Error("cannot get the task", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot get the task")
	case !found:
		return JSONProblemf(c, http.StatusNotFound, "scheduler task %s not found", taskName)
	case parent != "":
		return JSONProblemf(c, http.StatusBadRequest, "%s is a subtask of %s, which runs it: disable %s", taskName, parent, parent)
	}
	if err := a.ODB.SetSchedulerTaskDisabled(ctx, taskName, body.IsDisabled); err != nil {
		log.Error("cannot change the task", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot change the task")
	}
	action, verb := "scheduler.task.enable", "enabled"
	if body.IsDisabled {
		action, verb = "scheduler.task.disable", "disabled"
	}
	user, _ := c.Get(XUserEmail).(string)
	if err := a.ODB.Log(ctx, cdb.LogEntry{
		Action: action,
		User:   user,
		Fmt:    "scheduler task %(task)s " + verb + ".",
		Dict:   map[string]any{"task": taskName},
		Level:  "info",
	}); err != nil {
		log.Warn("cannot log the change", logkey.Error, err)
	}
	if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
		log.Warn("cannot notify changes", logkey.Error, err)
	}
	return a.getSchedulerTask(c, "PutSchedulerTask", taskName, nil)
}

// GetSchedulerRuns handles GET /scheduler/runs.
func (a *Api) GetSchedulerRuns(c echo.Context, params server.GetSchedulerRunsParams) error {
	// The tasks, their runs and their errors tell the inside of the
	// collector: a Manager only, as changing them.
	if err := requireManager(c); err != nil {
		return err
	}
	return a.handleList(c, "GetSchedulerRuns", "scheduler_run", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetSchedulerRuns(ctx, p)
	})
}
