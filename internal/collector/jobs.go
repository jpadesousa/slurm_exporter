package collector

import (
	"fmt"
	"strconv"
	"strings"

	"gitea.psi.ch/agosti_j/slurm_exporter/internal/logger"
	"github.com/prometheus/client_golang/prometheus"
)

// JobInfo holds information about a single job.
type JobInfo struct {
	Name      string
	Partition string
	Time      float64
	TimeLeft  float64
	TimeLimit float64
}

// JobsData executes squeue for a specific cluster and job state.
func JobsData(logger *logger.Logger, cluster string, state string) ([]byte, error) {
	// Now include %P for partition
	args := []string{"-a", "-h", "-o", "%i|%P|%j|%M|%L|%l", "--states=" + state}

	if cluster != "" {
		args = append([]string{"-M", cluster}, args...)
	}

	return Execute(logger, "squeue", args)
}

// durationToSeconds converts Slurm time formats into seconds.
func durationToSeconds(s string) (float64, error) {
	s = strings.TrimSpace(s)

	if s == "" || s == "N/A" {
		return 0, nil
	}

	// Days case: D-HH:MM:SS
	if strings.Contains(s, "-") {
		parts := strings.SplitN(s, "-", 2)

		if len(parts) != 2 {
			return 0, fmt.Errorf("invalid duration %q", s)
		}

		days, err := strconv.Atoi(parts[0])
		if err != nil {
			return 0, fmt.Errorf("invalid days %q: %w", parts[0], err)
		}

		rest, err := durationToSeconds(parts[1])
		if err != nil {
			return 0, err
		}

		return float64(days*86400) + rest, nil
	}

	parts := strings.Split(s, ":")

	switch len(parts) {
	case 3: // HH:MM:SS
		hours, err := strconv.Atoi(parts[0])
		if err != nil {
			return 0, fmt.Errorf("invalid hours %q: %w", parts[0], err)
		}

		minutes, err := strconv.Atoi(parts[1])
		if err != nil {
			return 0, fmt.Errorf("invalid minutes %q: %w", parts[1], err)
		}

		seconds, err := strconv.Atoi(parts[2])
		if err != nil {
			return 0, fmt.Errorf("invalid seconds %q: %w", parts[2], err)
		}

		if minutes >= 60 || seconds >= 60 {
			return 0, fmt.Errorf("invalid duration %q", s)
		}

		return float64(hours*3600 + minutes*60 + seconds), nil

	case 2: // MM:SS
		minutes, err := strconv.Atoi(parts[0])
		if err != nil {
			return 0, fmt.Errorf("invalid minutes %q: %w", parts[0], err)
		}

		seconds, err := strconv.Atoi(parts[1])
		if err != nil {
			return 0, fmt.Errorf("invalid seconds %q: %w", parts[1], err)
		}

		if seconds >= 60 {
			return 0, fmt.Errorf("invalid duration %q", s)
		}

		return float64(minutes*60 + seconds), nil

	case 1: // Seconds
		secs, err := strconv.ParseFloat(parts[0], 64)
		if err != nil {
			return 0, fmt.Errorf("invalid seconds %q: %w", s, err)
		}

		return secs, nil

	default:
		return 0, fmt.Errorf("invalid duration %q", s)
	}
}

// ParseJobsMetrics parses squeue output into a map of JobID -> JobInfo.
func ParseJobsMetrics(logger *logger.Logger, cluster string, state string) (map[string]*JobInfo, error) {
	jobs := make(map[string]*JobInfo)

	jobsData, err := JobsData(logger, cluster, state)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(jobsData), "\n")

	for _, line := range lines {
		if !strings.Contains(line, "|") {
			continue
		}

		parts := strings.Split(line, "|")

		if len(parts) < 6 {
			logger.Warn(
				"Skipping malformed job",
				"cluster", cluster,
				"line", line,
			)
			continue
		}

		jobid := strings.TrimSpace(parts[0])

		// Parse elapsed time.
		jobTime, err := durationToSeconds(strings.TrimSpace(parts[3]))
		if err != nil {
			logger.Warn(
				"Skipping job with invalid Time",
				"jobID", jobid,
				"value", parts[3],
				"error", err,
			)
			continue
		}

		// Parse remaining time.
		timeLeft, err := durationToSeconds(strings.TrimSpace(parts[4]))
		if err != nil {
			logger.Warn(
				"Skipping job with invalid TimeLeft",
				"jobID", jobid,
				"value", parts[4],
				"error", err,
			)
			continue
		}

		// Parse time limit.
		timeLimit, err := durationToSeconds(strings.TrimSpace(parts[5]))
		if err != nil {
			logger.Warn(
				"Skipping job with invalid TimeLimit",
				"jobID", jobid,
				"value", parts[5],
				"error", err,
			)
			continue
		}

		jobs[jobid] = &JobInfo{
			Partition: strings.TrimSpace(parts[1]),
			Name:      strings.TrimSpace(parts[2]),
			Time:      jobTime,
			TimeLeft:  timeLeft,
			TimeLimit: timeLimit,
		}
	}

	return jobs, nil
}

// JobsCollector collects job metrics, labeled by jobid and cluster.
type JobsCollector struct {
	clusters map[string]string

	// Running jobs metrics
	running         *prometheus.Desc
	runningTime     *prometheus.Desc
	runningTimeLeft *prometheus.Desc
	runningLimit    *prometheus.Desc
	runningLeftPct  *prometheus.Desc

	// Pending jobs metrics
	pending         *prometheus.Desc
	pendingTime     *prometheus.Desc
	pendingTimeLeft *prometheus.Desc
	pendingLimit    *prometheus.Desc

	logger *logger.Logger
}

// NewJobsCollector creates a new collector for job metrics.
func NewJobsCollector(logger *logger.Logger, clusters map[string]string) *JobsCollector {
	labels := []string{"jobid", "name", "partition", "slurm_cluster"}

	return &JobsCollector{
		clusters: clusters,

		// Running
		running: prometheus.NewDesc(
			"slurm_exporter_job_running",
			"Running jobs per jobid",
			labels, nil,
		),

		runningTime: prometheus.NewDesc(
			"slurm_exporter_job_running_time_seconds",
			"Elapsed job runtime in seconds (RUNNING only)",
			labels, nil,
		),

		runningTimeLeft: prometheus.NewDesc(
			"slurm_exporter_job_running_time_left_seconds",
			"Remaining job time in seconds (RUNNING only)",
			labels, nil,
		),

		runningLimit: prometheus.NewDesc(
			"slurm_exporter_job_running_time_limit_seconds",
			"Job time limit in seconds (RUNNING only)",
			labels, nil,
		),

		runningLeftPct: prometheus.NewDesc(
			"slurm_exporter_job_running_time_left_percent",
			"Remaining time as percentage of job time limit (RUNNING only)",
			labels, nil,
		),

		// Pending
		pending: prometheus.NewDesc(
			"slurm_exporter_job_pending",
			"Pending jobs per jobid",
			labels, nil,
		),

		pendingTime: prometheus.NewDesc(
			"slurm_exporter_job_pending_time_seconds",
			"Elapsed job runtime in seconds (PENDING usually 0)",
			labels, nil,
		),

		pendingTimeLeft: prometheus.NewDesc(
			"slurm_exporter_job_pending_time_left_seconds",
			"Remaining job time in seconds (PENDING usually 0)",
			labels, nil,
		),

		pendingLimit: prometheus.NewDesc(
			"slurm_exporter_job_pending_time_limit_seconds",
			"Job time limit in seconds (PENDING jobs)",
			labels, nil,
		),

		logger: logger,
	}
}

// Describe implements the prometheus.Collector interface.
func (jc *JobsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- jc.running
	ch <- jc.runningTime
	ch <- jc.runningTimeLeft
	ch <- jc.runningLimit
	ch <- jc.runningLeftPct

	ch <- jc.pending
	ch <- jc.pendingTime
	ch <- jc.pendingTimeLeft
	ch <- jc.pendingLimit
}

// Collect implements the prometheus.Collector interface.
func (jc *JobsCollector) Collect(ch chan<- prometheus.Metric) {
	for clusterLabel, cluster := range jc.clusters {

		// Running jobs
		runningJobs, err := ParseJobsMetrics(jc.logger, cluster, "RUNNING")
		if err == nil {
			for jobid, job := range runningJobs {
				labels := []string{jobid, job.Name, job.Partition, clusterLabel}

				ch <- prometheus.MustNewConstMetric(
					jc.running,
					prometheus.GaugeValue,
					1,
					labels...,
				)

				ch <- prometheus.MustNewConstMetric(
					jc.runningTime,
					prometheus.GaugeValue,
					job.Time,
					labels...,
				)

				ch <- prometheus.MustNewConstMetric(
					jc.runningTimeLeft,
					prometheus.GaugeValue,
					job.TimeLeft,
					labels...,
				)

				ch <- prometheus.MustNewConstMetric(
					jc.runningLimit,
					prometheus.GaugeValue,
					job.TimeLimit,
					labels...,
				)

				if job.TimeLimit > 0 {
					percent := (job.TimeLeft / job.TimeLimit) * 100.0

					ch <- prometheus.MustNewConstMetric(
						jc.runningLeftPct,
						prometheus.GaugeValue,
						percent,
						labels...,
					)
				}
			}
		} else {
			jc.logger.Error(
				"Failed to parse running jobs",
				"slurm_cluster", clusterLabel,
				"err", err,
			)
		}

		// Pending jobs
		pendingJobs, err := ParseJobsMetrics(jc.logger, cluster, "PENDING")
		if err == nil {
			for jobid, job := range pendingJobs {
				labels := []string{jobid, job.Name, job.Partition, clusterLabel}

				ch <- prometheus.MustNewConstMetric(
					jc.pending,
					prometheus.GaugeValue,
					1,
					labels...,
				)

				ch <- prometheus.MustNewConstMetric(
					jc.pendingTime,
					prometheus.GaugeValue,
					job.Time,
					labels...,
				)

				ch <- prometheus.MustNewConstMetric(
					jc.pendingTimeLeft,
					prometheus.GaugeValue,
					job.TimeLeft,
					labels...,
				)

				ch <- prometheus.MustNewConstMetric(
					jc.pendingLimit,
					prometheus.GaugeValue,
					job.TimeLimit,
					labels...,
				)
			}
		} else {
			jc.logger.Error(
				"Failed to parse pending jobs",
				"slurm_cluster", clusterLabel,
				"err", err,
			)
		}
	}
}
