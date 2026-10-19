package collector

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jpadesousa/slurm_exporter/internal/logger"
	"github.com/prometheus/client_golang/prometheus"
)

// JobEffInfo holds information about the efficiency metrics of a job.
type JobEffInfo struct {
	JobID     string
	Name      string
	Cluster   string
	Partition string
	User      string
	Group     string
	Account   string
	State     string
	Submit    string
	Eligible  string
	Start     string
	End       string

	ElapsedRaw   float64
	TimelimitRaw float64
	AllocNodes   float64
	AllocCPUS    float64
	CPUTimeRaw   float64

	TresUsageInTotCPU     float64
	TresUsageInTotEnergy  float64
	TresUsageInTotFSDisk  float64
	TresUsageOutTotFSDisk float64
	TresUsageInTotPages   float64
	TresUsageInTotMem     float64
	TresUsageInTotVMem    float64

	AllocMem float64

	// Derived values.
	TimeToScience          float64
	TimeToScienceValid     bool
	TimeInQueue            float64
	TimeInQueueValid       bool
	RunningToTimeToScience float64
	RunningToLimit         float64
	CPUEfficiency          float64
	MemoryEfficiency       float64
}

// JobEffData executes sacct for jobs finished within the configured interval.
func JobEffData(logger *logger.Logger, cluster string, timeBeforeNow string) ([]byte, error) {
	startTime := "now-" + timeBeforeNow

	args := []string{
		"-n",
		"-P",
		"-S", startTime,
		"-E", "now",
		"-s", "OUT_OF_MEMORY,COMPLETED,FAILED,TIMEOUT,CANCELLED",
		"-o", "JobID,JobName,Cluster,Partition,User,Group,Account," +
			"State,Submit,Eligible,Start,End,ElapsedRaw,TimelimitRaw," +
			"AllocNodes,AllocCPUS,CPUTimeRaw,AllocTRES,TresUsageInTot," +
			"TresUsageOutTot"}
	if cluster != "" {
		args = append([]string{"-M", cluster}, args...)
	}
	return Execute(logger, "sacct", args)
}

// parseMemory converts a memory value to bytes.
func parseMemory(s string) (float64, error) {
	s = strings.TrimPrefix(s, "*")

	if s == "" || s == "0" {
		return 0, nil
	}

	unit := s[len(s)-1]

	// No unit: value is in bytes.
	if unit >= '0' && unit <= '9' || unit == '.' {
		value, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, err
		}
		return value, nil
	}

	value, err := strconv.ParseFloat(s[:len(s)-1], 64)
	if err != nil {
		return 0, err
	}

	switch unit {
	case 'K':
		return value * 1024, nil
	case 'M':
		return value * 1024 * 1024, nil
	case 'G':
		return value * 1024 * 1024 * 1024, nil
	case 'T':
		return value * 1024 * 1024 * 1024 * 1024, nil
	default:
		return 0, fmt.Errorf("unknown memory unit: %q", unit)
	}
}

type TresUsage struct {
	CPU    float64
	Energy float64
	FSDisk float64
	Mem    float64
	VMem   float64
	Pages  float64
}

type AllocTRES struct {
	CPU  float64
	Mem  float64
	Node float64
}

func splitJobEffFields(line string, fields *[20]string) bool {
	for i := range fields {
		field, rest, found := strings.Cut(line, "|")
		fields[i] = field

		if !found {
			return i == len(fields)-1
		}

		line = rest
	}

	return true
}

// baseJobID removes the step suffix from a Slurm job ID.
func baseJobID(jobID string) string {
	base, _, ok := strings.Cut(jobID, ".")
	if ok {
		return base
	}
	return jobID
}

func parsePages(value string) (float64, error) {
	value = strings.ToUpper(value)

	multiplier := float64(1)

	switch {
	case strings.HasSuffix(value, "K"):
		multiplier = 1_000
		value = strings.TrimSuffix(value, "K")
	case strings.HasSuffix(value, "M"):
		multiplier = 1_000_000
		value = strings.TrimSuffix(value, "M")
	}

	v, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, err
	}

	return v * multiplier, nil
}

// parseAllocTRES parses the output from AllocTRES.
func parseAllocTRES(s string) (AllocTRES, error) {
	var result AllocTRES

	for s != "" {
		var item string

		item, s, _ = strings.Cut(s, ",")

		if item == "" {
			continue
		}

		key, value, ok := strings.Cut(item, "=")
		if !ok {
			continue
		}

		switch key {
		case "cpu":
			seconds, err := durationToSeconds(value)
			if err != nil {
				return result, fmt.Errorf("invalid cpu %q: %w", value, err)
			}
			result.CPU += seconds

		case "mem":
			v, err := parseMemory(value)
			if err != nil {
				return result, fmt.Errorf("invalid mem %q: %w", value, err)
			}
			result.Mem += v

		case "node":
			v, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return result, fmt.Errorf("invalid node %q: %w", value, err)
			}
			result.Node += v
		}
	}

	return result, nil
}

// parseTresUsage parses a Slurm TRES usage field.
func parseTresUsage(s string) (TresUsage, error) {
	var result TresUsage

	for s != "" {
		var item string

		item, s, _ = strings.Cut(s, ",")

		if item == "" {
			continue
		}

		key, value, ok := strings.Cut(item, "=")
		if !ok {
			continue
		}

		switch key {
		case "cpu":
			seconds, err := durationToSeconds(value)
			if err != nil {
				return result, fmt.Errorf("invalid cpu %q: %w", value, err)
			}
			result.CPU += seconds

		case "energy":
			v, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return result, fmt.Errorf("invalid energy %q: %w", value, err)
			}
			result.Energy += v

		case "fs/disk":
			v, err := parseMemory(value)
			if err != nil {
				return result, fmt.Errorf("invalid fs/disk %q: %w", value, err)
			}
			result.FSDisk += v

		case "mem":
			v, err := parseMemory(value)
			if err != nil {
				return result, fmt.Errorf("invalid mem %q: %w", value, err)
			}
			result.Mem = v

		case "vmem":
			v, err := parseMemory(value)
			if err != nil {
				return result, fmt.Errorf("invalid vmem %q: %w", value, err)
			}
			result.VMem = v

		case "pages":
			v, err := parsePages(value)
			if err != nil {
				return result, fmt.Errorf("invalid pages %q: %w", value, err)
			}
			result.Pages += v
		}
	}

	return result, nil
}

// ParseJobEffMetrics parses sacct output into a map of base job ID to JobEffInfo.
func ParseJobEffMetrics(logger *logger.Logger, cluster string, timeBeforeNow string) (map[string]*JobEffInfo, error) {
	jobEff := make(map[string]*JobEffInfo)

	jobEffData, err := JobEffData(logger, cluster, timeBeforeNow)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(bytes.NewReader(jobEffData))
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()

		var parts [20]string
		if !splitJobEffFields(line, &parts) {
			continue
		}

		rawJobID := parts[0]
		jobID := baseJobID(rawJobID)

		// Base job record.
		if rawJobID == jobID {
			elapsedRaw, err := strconv.ParseFloat(parts[12], 64)
			if err != nil {
				logger.Warn(
					"invalid elapsedRaw",
					"jobID", jobID,
					"value", parts[12],
					"error", err,
				)
				continue
			}

			var timelimitRaw float64

			if parts[13] != "" {
				timelimitRaw, err = strconv.ParseFloat(parts[13], 64)
				if err != nil {
					logger.Warn(
						"invalid timelimitRaw",
						"jobID", jobID,
						"value", parts[13],
						"error", err,
					)
					continue
				}
			} else {
				logger.Debug(
					"timelimitRaw unavailable",
					"jobID", jobID,
				)
			}

			timelimitRaw *= 60

			allocNodes, err := strconv.ParseFloat(parts[14], 64)
			if err != nil {
				logger.Warn(
					"invalid allocNodes",
					"jobID", jobID,
					"value", parts[14],
					"error", err,
				)
				continue
			}

			allocCPUS, err := strconv.ParseFloat(parts[15], 64)
			if err != nil {
				logger.Warn(
					"invalid allocCPUS",
					"jobID", jobID,
					"value", parts[15],
					"error", err,
				)
				continue
			}

			cpuTimeRaw, err := strconv.ParseFloat(parts[16], 64)
			if err != nil {
				logger.Warn(
					"invalid cpuTimeRaw",
					"jobID", jobID,
					"value", parts[16],
					"error", err,
				)
				continue
			}

			jobEff[jobID] = &JobEffInfo{
				JobID:        jobID,
				Name:         parts[1],
				Cluster:      parts[2],
				Partition:    parts[3],
				User:         parts[4],
				Group:        parts[5],
				Account:      parts[6],
				State:        parts[7],
				Submit:       parts[8],
				Eligible:     parts[9],
				Start:        parts[10],
				End:          parts[11],
				ElapsedRaw:   elapsedRaw,
				TimelimitRaw: timelimitRaw,
				AllocNodes:   allocNodes,
				AllocCPUS:    allocCPUS,
				CPUTimeRaw:   cpuTimeRaw,
			}

			continue
		}

		// Step record.
		job, exists := jobEff[jobID]
		if !exists {
			continue
		}

		// Exclude .extern entries.
		if strings.HasSuffix(rawJobID, ".extern") {
			continue
		}

		TresUsageIn, err := parseTresUsage(parts[18])
		if err != nil {
			logger.Warn(
				"invalid tresUsage",
				"jobID", jobID,
				"rawJobID", rawJobID,
				"value", parts[18],
				"error", err,
			)
			continue
		}

		job.TresUsageInTotCPU += TresUsageIn.CPU
		job.TresUsageInTotEnergy += TresUsageIn.Energy
		job.TresUsageInTotFSDisk += TresUsageIn.FSDisk
		job.TresUsageInTotPages += TresUsageIn.Pages
		job.TresUsageInTotMem += TresUsageIn.Mem
		job.TresUsageInTotVMem += TresUsageIn.VMem

		TresUsageOut, err := parseTresUsage(parts[19])
		if err != nil {
			logger.Warn(
				"invalid tresUsage",
				"jobID", jobID,
				"rawJobID", rawJobID,
				"value", parts[19],
				"error", err,
			)
			continue
		}

		job.TresUsageOutTotFSDisk += TresUsageOut.FSDisk

		allocTres, err := parseAllocTRES(parts[17])
		if err != nil {
			logger.Warn(
				"invalid allocTres",
				"jobID", jobID,
				"rawJobID", rawJobID,
				"value", parts[17],
				"error", err,
			)
			continue
		}

		job.AllocMem += allocTres.Mem
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning sacct output: %w", err)
	}

	return jobEff, nil
}

type JobEffCollector struct {
	mu      sync.RWMutex
	metrics map[string]map[string]*JobEffInfo

	clusters      map[string]string
	timeBeforeNow string
	refreshTime   int64

	timeAllocated               *prometheus.Desc
	coresAllocated              *prometheus.Desc
	memoryAllocated             *prometheus.Desc
	cpuTime                     *prometheus.Desc
	cpuUsed                     *prometheus.Desc
	memUsed                     *prometheus.Desc
	timeToScience               *prometheus.Desc
	timeInQueue                 *prometheus.Desc
	timeRunning                 *prometheus.Desc
	runningToTimeToScienceRatio *prometheus.Desc
	runningToLimitRatio         *prometheus.Desc
	cpuEfficiency               *prometheus.Desc
	memoryEfficiency            *prometheus.Desc
	InIoUsed                    *prometheus.Desc
	OutIoUsed                   *prometheus.Desc
	energyUsed                  *prometheus.Desc
	pagesUsed                   *prometheus.Desc
	vmemUsed                    *prometheus.Desc

	logger *logger.Logger
}

// NewJobEffCollector creates a new collector for job efficiency metrics.
func NewJobEffCollector(logger *logger.Logger, clusters map[string]string, timeBeforeNow string, refreshTime int64) *JobEffCollector {
	labels := []string{"jobid", "name", "slurm_cluster", "partition", "user", "group", "account", "state"}
	return &JobEffCollector{
		clusters:      clusters,
		timeBeforeNow: timeBeforeNow,
		refreshTime:   refreshTime,

		timeAllocated: prometheus.NewDesc(
			"slurm_exporter_job_eff_allocated_time_seconds",
			"Amount of wall-clock time allocated for the job in seconds.",
			labels, nil,
		),

		coresAllocated: prometheus.NewDesc(
			"slurm_exporter_job_eff_allocated_cores",
			"Number of CPU cores allocated by the job.",
			labels, nil,
		),

		memoryAllocated: prometheus.NewDesc(
			"slurm_exporter_job_eff_allocated_memory_bytes",
			"Amount of memory allocated by the job in bytes.",
			labels, nil,
		),

		cpuTime: prometheus.NewDesc(
			"slurm_exporter_job_eff_allocated_cpu_time_seconds",
			"Total CPU time allocated to the job in seconds.",
			labels, nil,
		),

		cpuUsed: prometheus.NewDesc(
			"slurm_exporter_job_eff_used_cpu_time_seconds",
			"Total CPU time used by the job in seconds.",
			labels, nil,
		),

		memUsed: prometheus.NewDesc(
			"slurm_exporter_job_eff_used_memory_bytes",
			"Amount of memory used by the job in bytes.",
			labels, nil,
		),

		timeToScience: prometheus.NewDesc(
			"slurm_exporter_job_eff_time_to_science_seconds",
			"Time from job eligibility to job completion in seconds.",
			labels, nil,
		),

		timeInQueue: prometheus.NewDesc(
			"slurm_exporter_job_eff_time_in_queue_seconds",
			"Time from job eligibility to job start in seconds.",
			labels, nil,
		),

		timeRunning: prometheus.NewDesc(
			"slurm_exporter_job_eff_time_running_seconds",
			"Time from job start to job completion in seconds.",
			labels, nil,
		),

		runningToTimeToScienceRatio: prometheus.NewDesc(
			"slurm_exporter_job_eff_running_to_time_to_science_ratio",
			"Ratio of job running time to total job time since it was eligible to run.",
			labels, nil,
		),

		runningToLimitRatio: prometheus.NewDesc(
			"slurm_exporter_job_eff_running_to_time_limit_ratio",
			"Ratio of job running time to its allocated time limit.",
			labels, nil,
		),

		cpuEfficiency: prometheus.NewDesc(
			"slurm_exporter_job_eff_cpu_efficiency_ratio",
			"Ratio of CPU time consumed to allocated CPU time.",
			labels, nil,
		),

		memoryEfficiency: prometheus.NewDesc(
			"slurm_exporter_job_eff_memory_efficiency_ratio",
			"Ratio of memory used to memory allocated to the job.",
			labels, nil,
		),

		InIoUsed: prometheus.NewDesc(
			"slurm_exporter_job_eff_in_io_bytes_total",
			"Total filesystem input I/O performed by the job in bytes.",
			labels, nil,
		),

		OutIoUsed: prometheus.NewDesc(
			"slurm_exporter_job_eff_out_io_bytes_total",
			"Total filesystem output I/O performed by the job in bytes.",
			labels, nil,
		),

		energyUsed: prometheus.NewDesc(
			"slurm_exporter_job_eff_energy_joules_total",
			"Total energy consumed by the job in joules.",
			labels, nil,
		),

		pagesUsed: prometheus.NewDesc(
			"slurm_exporter_job_eff_pages_total",
			"Total number of page faults incurred by the job.",
			labels, nil,
		),

		vmemUsed: prometheus.NewDesc(
			"slurm_exporter_job_eff_virtual_memory_bytes",
			"Virtual memory used by the job in bytes.",
			labels, nil,
		),
		logger: logger,
	}
}

// Describe implements prometheus.Collector.
func (jc *JobEffCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- jc.timeAllocated
	ch <- jc.coresAllocated
	ch <- jc.memoryAllocated
	ch <- jc.cpuTime
	ch <- jc.cpuUsed
	ch <- jc.memUsed
	ch <- jc.timeToScience
	ch <- jc.timeInQueue
	ch <- jc.timeRunning
	ch <- jc.runningToTimeToScienceRatio
	ch <- jc.runningToLimitRatio
	ch <- jc.cpuEfficiency
	ch <- jc.memoryEfficiency
	ch <- jc.InIoUsed
	ch <- jc.OutIoUsed
	ch <- jc.energyUsed
	ch <- jc.pagesUsed
	ch <- jc.vmemUsed
}

func parseTimestamp(s string) (time.Time, bool) {
	t, err := time.Parse("2006-01-02T15:04:05", s)
	if err != nil {
		return time.Time{}, false
	}

	return t, true
}

// Start starts the background refresh.
//
// The metrics are refreshed immediately and then once per hour.
func (jc *JobEffCollector) Start() {
	jc.logger.Info("Starting JobEffCollector")

	go func() {
		// Refresh immediately at startup.
		jc.refresh()

		ticker := time.NewTicker(time.Duration(jc.refreshTime) * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			jc.logger.Info("Refreshing JobEffCollector")
			jc.refresh()
		}
	}()
}

// refresh gets fresh data from Slurm and replaces the cached metrics.
func (jc *JobEffCollector) refresh() {
	jc.logger.Info("Refreshing job efficiency metrics")

	metrics := make(map[string]map[string]*JobEffInfo)

	for clusterLabel, cluster := range jc.clusters {
		jc.logger.Info(
			"Parsing job efficiency",
			"cluster", clusterLabel,
		)

		jobMetrics, err := ParseJobEffMetrics(jc.logger, cluster, jc.timeBeforeNow)
		if err != nil {
			jc.logger.Error(
				"Failed to parse job efficiency",
				"slurm_cluster", clusterLabel,
				"err", err,
			)
			continue
		}

		for _, job := range jobMetrics {
			calculateJobMetrics(jc.logger, job)
		}

		jc.logger.Info(
			"Parsed job efficiency",
			"cluster", clusterLabel,
			"jobs", len(jobMetrics),
		)

		metrics[clusterLabel] = jobMetrics
	}

	jc.mu.Lock()
	jc.metrics = metrics
	jc.mu.Unlock()

	jc.logger.Info(
		"Job efficiency cache updated",
		"clusters", len(metrics),
	)
}

func calculateJobMetrics(logger *logger.Logger, job *JobEffInfo) {
	end, endOK := parseTimestamp(job.End)
	eligible, eligibleOK := parseTimestamp(job.Eligible)

	if endOK && eligibleOK {
		job.TimeToScience = end.Sub(eligible).Seconds()
		job.TimeToScienceValid = true

		if job.TimeToScience != 0 {
			job.RunningToTimeToScience =
				job.ElapsedRaw / job.TimeToScience
		}
	} else {
		logger.Warn(
			"Invalid time to science",
			"job_id", job.JobID,
			"end", job.End,
			"eligible", job.Eligible,
		)
	}

	start, startOK := parseTimestamp(job.Start)

	if startOK && eligibleOK {
		job.TimeInQueue = start.Sub(eligible).Seconds()
		job.TimeInQueueValid = true
	} else {
		logger.Warn(
			"Invalid time in queue",
			"job_id", job.JobID,
			"start", job.Start,
			"eligible", job.Eligible,
		)
	}

	if job.TimelimitRaw != 0 {
		job.RunningToLimit = job.ElapsedRaw / job.TimelimitRaw
	}

	if job.CPUTimeRaw != 0 {
		job.CPUEfficiency = job.TresUsageInTotCPU / job.CPUTimeRaw
	}

	if job.AllocMem != 0 {
		job.MemoryEfficiency = job.TresUsageInTotMem / job.AllocMem
	}
}

// Collect implements prometheus.Collector.
func (jc *JobEffCollector) Collect(ch chan<- prometheus.Metric) {
	jc.mu.RLock()
	defer jc.mu.RUnlock()

	for clusterLabel, jobEffMetrics := range jc.metrics {
		cluster := jc.clusters[clusterLabel]

		for jobid, job := range jobEffMetrics {
			labels := []string{
				jobid,
				job.Name,
				cluster,
				job.Partition,
				job.User,
				job.Group,
				job.Account,
				job.State,
			}

			ch <- prometheus.MustNewConstMetric(
				jc.timeAllocated,
				prometheus.GaugeValue,
				job.TimelimitRaw,
				labels...,
			)

			ch <- prometheus.MustNewConstMetric(
				jc.coresAllocated,
				prometheus.GaugeValue,
				job.AllocCPUS,
				labels...,
			)

			ch <- prometheus.MustNewConstMetric(
				jc.memoryAllocated,
				prometheus.GaugeValue,
				job.AllocMem,
				labels...,
			)

			ch <- prometheus.MustNewConstMetric(
				jc.cpuTime,
				prometheus.GaugeValue,
				job.CPUTimeRaw,
				labels...,
			)

			ch <- prometheus.MustNewConstMetric(
				jc.cpuUsed,
				prometheus.GaugeValue,
				job.TresUsageInTotCPU,
				labels...,
			)

			ch <- prometheus.MustNewConstMetric(
				jc.memUsed,
				prometheus.GaugeValue,
				job.TresUsageInTotMem,
				labels...,
			)

			ch <- prometheus.MustNewConstMetric(
				jc.timeToScience,
				prometheus.GaugeValue,
				job.TimeToScience,
				labels...,
			)

			ch <- prometheus.MustNewConstMetric(
				jc.timeInQueue,
				prometheus.GaugeValue,
				job.TimeInQueue,
				labels...,
			)

			ch <- prometheus.MustNewConstMetric(
				jc.timeRunning,
				prometheus.GaugeValue,
				job.ElapsedRaw,
				labels...,
			)

			ch <- prometheus.MustNewConstMetric(
				jc.runningToTimeToScienceRatio,
				prometheus.GaugeValue,
				job.RunningToTimeToScience,
				labels...,
			)

			ch <- prometheus.MustNewConstMetric(
				jc.runningToLimitRatio,
				prometheus.GaugeValue,
				job.RunningToLimit,
				labels...,
			)

			ch <- prometheus.MustNewConstMetric(
				jc.cpuEfficiency,
				prometheus.GaugeValue,
				job.CPUEfficiency,
				labels...,
			)

			ch <- prometheus.MustNewConstMetric(
				jc.memoryEfficiency,
				prometheus.GaugeValue,
				job.MemoryEfficiency,
				labels...,
			)

			ch <- prometheus.MustNewConstMetric(
				jc.InIoUsed,
				prometheus.GaugeValue,
				job.TresUsageInTotFSDisk,
				labels...,
			)

			ch <- prometheus.MustNewConstMetric(
				jc.OutIoUsed,
				prometheus.GaugeValue,
				job.TresUsageOutTotFSDisk,
				labels...,
			)

			ch <- prometheus.MustNewConstMetric(
				jc.energyUsed,
				prometheus.GaugeValue,
				job.TresUsageInTotEnergy,
				labels...,
			)

			ch <- prometheus.MustNewConstMetric(
				jc.pagesUsed,
				prometheus.GaugeValue,
				job.TresUsageInTotPages,
				labels...,
			)

			ch <- prometheus.MustNewConstMetric(
				jc.vmemUsed,
				prometheus.GaugeValue,
				job.TresUsageInTotVMem,
				labels...,
			)
		}
	}
}
