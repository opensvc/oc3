package cdb

import (
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metrics *Metrics
)

func InitMetrics() {
	metrics = newMetrics()
}

func newMetrics() *Metrics {
	const (
		// db operation statuses
		dbOpDeadlock = "deadlock"
		dbOpError    = "error"
		dbOpFailure  = "failure"
		dbOpRetry    = "retry"
		dbOpSuccess  = "success"

		// db operations
		dbCommit   = "commit"
		dbRollback = "rollback"
		dbExec     = "exec"
		dbExecTx   = "exec_tx"
		dbBeginTx  = "begin_tx"

		// data operations
		dataUpdate = "update"

		// status log operations
		logChange = "change"
		logExtend = "extend"
		logInsert = "insert"

		// data types
		hbStatus = "heartbeat_status"
		iStatus  = "instance_status"
		oStatus  = "object_status"
		rStatus  = "resource_status"
	)

	dbOpCount := promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "oc3",
			Subsystem: "db",
			Name:      "sql_operations_total",
			Help: fmt.Sprintf("Total number of database operations (operation={%s}, status={%s})",
				strings.Join([]string{dbCommit, dbRollback, dbExec, dbExecTx, dbBeginTx}, "|"),
				strings.Join([]string{dbOpDeadlock, dbOpError, dbOpFailure, dbOpRetry, dbOpSuccess}, "|")),
		},
		[]string{"operation", "status"},
	)

	logCount := promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "oc3",
			Subsystem: "db",
			Name:      "status_log_operations_total",
			Help: fmt.Sprintf("Total number of data status log operations (datatype={%s}, operation={%s})",
				strings.Join([]string{hbStatus, iStatus, oStatus, rStatus}, "|"),
				strings.Join([]string{logChange, logExtend, logInsert}, "|")),
		},
		[]string{"datatype", "operation"},
	)

	dataCount := promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "oc3",
			Subsystem: "db",
			Name:      "data_operations_total",
			Help: fmt.Sprintf("Total number of data operations (datatype={%s}, operation={%s})",
				strings.Join([]string{hbStatus, iStatus, oStatus, rStatus}, "|"),
				strings.Join([]string{dataUpdate}, "|")),
		},
		[]string{"datatype", "operation"},
	)

	return &Metrics{
		ExecErr:        dbOpCount.With(prometheus.Labels{"operation": dbExec, "status": dbOpError}),
		ExecOk:         dbOpCount.With(prometheus.Labels{"operation": dbExec, "status": dbOpSuccess}),
		BeginTxErr:     dbOpCount.With(prometheus.Labels{"operation": dbBeginTx, "status": dbOpError}),
		BeginTxOk:      dbOpCount.With(prometheus.Labels{"operation": dbBeginTx, "status": dbOpSuccess}),
		CommitErr:      dbOpCount.With(prometheus.Labels{"operation": dbCommit, "status": dbOpError}),
		CommitOk:       dbOpCount.With(prometheus.Labels{"operation": dbCommit, "status": dbOpSuccess}),
		RollbackErr:    dbOpCount.With(prometheus.Labels{"operation": dbRollback, "status": dbOpError}),
		RollbackOk:     dbOpCount.With(prometheus.Labels{"operation": dbRollback, "status": dbOpSuccess}),
		ExecTxErr:      dbOpCount.With(prometheus.Labels{"operation": dbExecTx, "status": dbOpError}),
		ExecTxOk:       dbOpCount.With(prometheus.Labels{"operation": dbExecTx, "status": dbOpSuccess}),
		ExecTxDeadlock: dbOpCount.With(prometheus.Labels{"operation": dbExecTx, "status": dbOpDeadlock}),
		ExecTxFailed:   dbOpCount.With(prometheus.Labels{"operation": dbExecTx, "status": dbOpFailure}),
		ExecTxRetry:    dbOpCount.With(prometheus.Labels{"operation": dbExecTx, "status": dbOpRetry}),

		HbStatusUpdate:       dataCount.With(prometheus.Labels{"datatype": hbStatus, "operation": dataUpdate}),
		ObjectStatusUpdate:   dataCount.With(prometheus.Labels{"datatype": oStatus, "operation": dataUpdate}),
		ResourceStatusUpdate: dataCount.With(prometheus.Labels{"datatype": rStatus, "operation": dataUpdate}),
		InstanceStatusUpdate: dataCount.With(prometheus.Labels{"datatype": iStatus, "operation": dataUpdate}),

		HbStatusLogChange:       logCount.With(prometheus.Labels{"datatype": hbStatus, "operation": logChange}),
		HbStatusLogExtend:       logCount.With(prometheus.Labels{"datatype": hbStatus, "operation": logExtend}),
		HbStatusLogInsert:       logCount.With(prometheus.Labels{"datatype": hbStatus, "operation": logInsert}),
		ObjectStatusLogChange:   logCount.With(prometheus.Labels{"datatype": oStatus, "operation": logChange}),
		ObjectStatusLogExtend:   logCount.With(prometheus.Labels{"datatype": oStatus, "operation": logExtend}),
		ObjectStatusLogInsert:   logCount.With(prometheus.Labels{"datatype": oStatus, "operation": logInsert}),
		ResourceStatusLogChange: logCount.With(prometheus.Labels{"datatype": rStatus, "operation": logChange}),
		ResourceStatusLogExtend: logCount.With(prometheus.Labels{"datatype": rStatus, "operation": logExtend}),
		ResourceStatusLogInsert: logCount.With(prometheus.Labels{"datatype": rStatus, "operation": logInsert}),
		InstanceStatusLogChange: logCount.With(prometheus.Labels{"datatype": iStatus, "operation": logChange}),
		InstanceStatusLogExtend: logCount.With(prometheus.Labels{"datatype": iStatus, "operation": logExtend}),
		InstanceStatusLogInsert: logCount.With(prometheus.Labels{"datatype": iStatus, "operation": logInsert}),
	}
}
