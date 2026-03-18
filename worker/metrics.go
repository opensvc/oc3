package worker

import (
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Total number of feed jobs executed
	feedJobCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "oc3",
			Name:      "feed_job_executions_total",
			Help: fmt.Sprintf("Total number of feed job executions (job_type={%s}, status={%s})",
				strings.Join([]string{jtDaemonPing, jtDaemonStatus, jtNodeSystem, jtInstanceAction, jtInstanceResourceInfo, jtInstanceStatus, jtNodeDisk, jtObjectConfig}, "|"),
				strings.Join([]string{jobStatusFailed, jobStatusOk}, "|")),
		},
		[]string{"job_type", "status"},
	)

	// Duration of entire feed jobs
	feedJobDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "oc3",
			Name:      "feed_job_duration_seconds",
			Help: fmt.Sprintf("Duration of entire feed job executions in seconds (job_type={%s}, status={%s})",
				strings.Join([]string{jtDaemonPing, jtDaemonStatus, jtNodeSystem, jtInstanceAction, jtInstanceResourceInfo, jtInstanceStatus, jtNodeDisk, jtObjectConfig}, "|"),
				strings.Join([]string{jobStatusFailed, jobStatusOk}, "|")),
			Buckets: prometheus.DefBuckets,
		},
		[]string{"job_type", "status"},
	)

	// Duration of individual steps within jobs
	feedJobStepDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "oc3",
			Name:      "feed_job_step_duration_seconds",
			Help: fmt.Sprintf("Duration of individual steps within feed jobs in seconds (job_type={%s}, status={%s}, job_step={main|...}})",
				strings.Join([]string{jtDaemonPing, jtDaemonStatus, jtNodeSystem, jtInstanceAction, jtInstanceResourceInfo, jtInstanceStatus, jtNodeDisk, jtObjectConfig}, "|"),
				strings.Join([]string{jobStatusFailed, jobStatusOk}, "|")),
			Buckets: prometheus.DefBuckets,
		},
		[]string{"job_type", "job_step", "status"},
	)
)
