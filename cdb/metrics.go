package cdb

import (
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type (
	Metrics struct {
		ExecErr        prometheus.Counter
		ExecOk         prometheus.Counter
		BeginTxErr     prometheus.Counter
		BeginTxOk      prometheus.Counter
		CommitErr      prometheus.Counter
		CommitOk       prometheus.Counter
		RollbackErr    prometheus.Counter
		RollbackOk     prometheus.Counter
		ExecTxErr      prometheus.Counter
		ExecTxOk       prometheus.Counter
		ExecTxDeadlock prometheus.Counter

		ExecTxRetry  prometheus.Counter
		ExecTxFailed prometheus.Counter

		// data metrics
		HbStatusUpdate prometheus.Counter

		InstanceActionInsert prometheus.Counter
		InstanceActionUpdate prometheus.Counter
		InstanceStatusUpdate prometheus.Counter

		ObjectConfigUpdate prometheus.Counter
		ObjectConfigDelete prometheus.Counter
		ObjectConfigInsert prometheus.Counter
		ObjectStatusUpdate prometheus.Counter

		NodeDiskUpdate   prometheus.Counter
		NodeConfigInsert prometheus.Counter

		ResourceInfoUpdate   prometheus.Counter
		ResourceInfoDelete   prometheus.Counter
		ResourceStatusDelete prometheus.Counter
		ResourceStatusUpdate prometheus.Counter

		// status log metrics
		HbStatusLogChange prometheus.Counter
		HbStatusLogExtend prometheus.Counter

		InstanceStatusLogChange prometheus.Counter
		InstanceStatusLogExtend prometheus.Counter

		ObjectStatusLogChange prometheus.Counter
		ObjectStatusLogExtend prometheus.Counter

		ResourceStatusLogChange prometheus.Counter
		ResourceStatusLogExtend prometheus.Counter
	}
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
		dataDelete = "delete"
		dataUpdate = "update"
		dataInsert = "insert"

		// status log operations
		logChange = "change"
		logExtend = "extend"

		// data types
		hbStatus = "heartbeat_status"
		iAction  = "instance_action"
		iStatus  = "instance_status"
		nConfig  = "node_config"
		nDisk    = "node_disk"
		oConfig  = "object_config"
		oStatus  = "object_status"
		rInfo    = "resource_info"
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
				strings.Join([]string{hbStatus, iAction, iStatus, nConfig, oStatus, rInfo, rStatus}, "|"),
				strings.Join([]string{logChange, logExtend}, "|")),
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

		// data metrics
		HbStatusUpdate: dataCount.With(prometheus.Labels{"datatype": hbStatus, "operation": dataUpdate}),

		InstanceActionInsert: dataCount.With(prometheus.Labels{"datatype": iAction, "operation": dataInsert}),
		InstanceActionUpdate: dataCount.With(prometheus.Labels{"datatype": iAction, "operation": dataUpdate}),
		InstanceStatusUpdate: dataCount.With(prometheus.Labels{"datatype": iStatus, "operation": dataUpdate}),

		NodeConfigInsert: dataCount.With(prometheus.Labels{"datatype": nConfig, "operation": dataUpdate}),
		NodeDiskUpdate:   dataCount.With(prometheus.Labels{"datatype": nDisk, "operation": dataUpdate}),

		ObjectConfigInsert: dataCount.With(prometheus.Labels{"datatype": oConfig, "operation": dataInsert}),
		ObjectConfigUpdate: dataCount.With(prometheus.Labels{"datatype": oConfig, "operation": dataUpdate}),
		ObjectStatusUpdate: dataCount.With(prometheus.Labels{"datatype": oStatus, "operation": dataUpdate}),

		ResourceInfoDelete:   dataCount.With(prometheus.Labels{"datatype": rInfo, "operation": dataDelete}),
		ResourceInfoUpdate:   dataCount.With(prometheus.Labels{"datatype": rInfo, "operation": dataUpdate}),
		ResourceStatusUpdate: dataCount.With(prometheus.Labels{"datatype": rStatus, "operation": dataUpdate}),
		ResourceStatusDelete: dataCount.With(prometheus.Labels{"datatype": rStatus, "operation": dataDelete}),

		// status log metrics
		HbStatusLogChange: logCount.With(prometheus.Labels{"datatype": hbStatus, "operation": logChange}),
		HbStatusLogExtend: logCount.With(prometheus.Labels{"datatype": hbStatus, "operation": logExtend}),

		InstanceStatusLogChange: logCount.With(prometheus.Labels{"datatype": iStatus, "operation": logChange}),
		InstanceStatusLogExtend: logCount.With(prometheus.Labels{"datatype": iStatus, "operation": logExtend}),

		ObjectStatusLogChange: logCount.With(prometheus.Labels{"datatype": oStatus, "operation": logChange}),
		ObjectStatusLogExtend: logCount.With(prometheus.Labels{"datatype": oStatus, "operation": logExtend}),

		ResourceStatusLogChange: logCount.With(prometheus.Labels{"datatype": rStatus, "operation": logChange}),
		ResourceStatusLogExtend: logCount.With(prometheus.Labels{"datatype": rStatus, "operation": logExtend}),
	}
}
