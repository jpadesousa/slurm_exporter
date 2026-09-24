package collector

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sckyzo/slurm_exporter/internal/logger"
)

// JobEffInfo holds information about the efficiency metrics of a single job.
type JobEffInfo struct {
	JobID                 string
	Name                  string
	Cluster               string
	Partition             string
	User                  string
	Group                 string
	Account               string
	State                 string
	Submit                string
	Eligible              string
	Start                 string
	End                   string
	ElapsedRaw            float64
	TimelimitRaw          float64
	AllocNodes            float64
	AllocCPUS             float64
	CPUTimeRaw            float64
	AllocMem              float64
	TresUsageInTotCPU     float64
	TresUsageInTotEnergy  float64
	TresUsageInTotFSDisk  float64
	TresUsageInTotMem     float64
	TresUsageInTotPages   float64
	TresUsageInTotVMem    float64
	TresUsageOutTotFSDisk float64
}

// JobEffData executes sacct command for jobs finished in a specified interval.
func JobEffData(logger *logger.Logger, cluster string) ([]byte, error) {
	args := []string{
		"-n",
		"-P",
		"-S", "now-1hour",
		"-E", "now",
		"-s", "OUT_OF_MEMORY,COMPLETED,FAILED,TIMEOUT,CANCELLED",
		"-o", "JobID,JobName,Cluster,Partition,User,Group,Account,State,Submit,Eligible,Start,End,ElapsedRaw,TimelimitRaw,AllocNodes,AllocCPUS,CPUTimeRaw,AllocTRES,TresUsageInTot,TresUsageOutTot"}
	if cluster != "" {
		args = append([]string{"-M", cluster}, args...)
	}
	return Execute(logger, "sacct", args)
}

// parseMemory converts all memory units to bytes
func parseMemory(s string) (float64, error) {
	s = strings.TrimSpace(s)
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

// baseJobID provides a base job ID by removing the suffix of the job ID.
func baseJobID(jobID string) string {
	if idx := strings.Index(jobID, "."); idx >= 0 {
		return jobID[:idx]
	}
	return jobID
}

func parsePages(value string) (float64, error) {
	value = strings.TrimSpace(strings.ToUpper(value))

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

	for _, item := range strings.Split(strings.TrimSpace(s), ",") {
		if item == "" {
			continue
		}

		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := parts[0]
		value := strings.TrimSpace(parts[1])

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

// parseTresUsage parses the output from TresUsageInTot and TresUsageOutTot.
func parseTresUsage(s string) (TresUsage, error) {
	var result TresUsage

	for _, item := range strings.Split(strings.TrimSpace(s), ",") {
		if item == "" {
			continue
		}

		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := parts[0]
		value := strings.TrimSpace(parts[1])

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

func subtractTime(a, b string) (float64, error) {
	t1, err := time.Parse("2006-01-02T15:04:05", a)
	if err != nil {
		return 0, err
	}

	t2, err := time.Parse("2006-01-02T15:04:05", b)
	if err != nil {
		return 0, err
	}

	return t1.Sub(t2).Seconds(), nil
}

// ParseJobEffMetrics parses sacct output into a map of JobID -> JobEffInfo.
func ParseJobEffMetrics(logger *logger.Logger, cluster string) (map[string]*JobEffInfo, error) {
	jobEff := make(map[string]*JobEffInfo)

	jobEffData, err := JobEffData(logger, cluster)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(jobEffData), "\n")

	for _, line := range lines {
		if !strings.Contains(line, "|") {
			continue
		}

		parts := strings.Split(line, "|")

		rawJobID := strings.TrimSpace(parts[0])
		jobID := baseJobID(rawJobID)

		// Base job record.
		if rawJobID == jobID {

			elapsedRaw, err := strconv.ParseFloat(strings.TrimSpace(parts[12]), 64)
			if err != nil {
				return nil, fmt.Errorf(
					"job %s: invalid elapsedRaw %q: %w",
					jobID,
					parts[12],
					err,
				)
			}

			timelimitRaw, err := strconv.ParseFloat(strings.TrimSpace(parts[13]), 64)
			if err != nil {
				return nil, fmt.Errorf(
					"job %s: invalid timelimitRaw %q: %w",
					jobID,
					parts[13],
					err,
				)
			} else {
				// timelimitRaw is given in minutes. Converting to seconds.
				timelimitRaw = timelimitRaw * 60
			}

			allocNodes, err := strconv.ParseFloat(strings.TrimSpace(parts[14]), 64)
			if err != nil {
				return nil, fmt.Errorf(
					"job %s: invalid allocNodes %q: %w",
					jobID,
					parts[14],
					err,
				)
			}

			allocCPUS, err := strconv.ParseFloat(strings.TrimSpace(parts[15]), 64)
			if err != nil {
				return nil, fmt.Errorf(
					"job %s: invalid allocCPUS %q: %w",
					jobID,
					parts[15],
					err,
				)
			}

			cpuTimeRaw, err := strconv.ParseFloat(strings.TrimSpace(parts[16]), 64)
			if err != nil {
				return nil, fmt.Errorf(
					"job %s: invalid elapsedRaw %q: %w",
					jobID,
					parts[16],
					err,
				)
			}

			jobEff[jobID] = &JobEffInfo{
				JobID:        jobID,
				Name:         strings.TrimSpace(parts[1]),
				Cluster:      strings.TrimSpace(parts[2]),
				Partition:    strings.TrimSpace(parts[3]),
				User:         strings.TrimSpace(parts[4]),
				Group:        strings.TrimSpace(parts[5]),
				Account:      strings.TrimSpace(parts[6]),
				State:        strings.TrimSpace(parts[7]),
				Submit:       strings.TrimSpace(parts[8]),
				Eligible:     strings.TrimSpace(parts[9]),
				Start:        strings.TrimSpace(parts[10]),
				End:          strings.TrimSpace(parts[11]),
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
			// Depending on your sacct output/order, you may want
			// to temporarily store step data here. Normally the
			// base job appears before its steps.
			continue
		}

		// isBatch := strings.HasSuffix(rawJobID, ".batch")
		isExtern := strings.HasSuffix(rawJobID, ".extern")

		// Exclude .extern and .batch entries
		if isExtern {
			continue
		}

		tresUsage, err := parseTresUsage(
			strings.TrimSpace(parts[18]),
		)
		if err != nil {
			return nil, fmt.Errorf(
				"job %s (%s): %w",
				jobID,
				rawJobID,
				err,
			)
		}

		job.TresUsageInTotCPU += tresUsage.CPU
		job.TresUsageInTotEnergy += tresUsage.Energy
		job.TresUsageInTotFSDisk += tresUsage.FSDisk
		job.TresUsageInTotPages += tresUsage.Pages
		job.TresUsageInTotMem += tresUsage.Mem
		job.TresUsageInTotVMem += tresUsage.VMem

		allocTres, err := parseAllocTRES(
			strings.TrimSpace(parts[17]),
		)
		if err != nil {
			return nil, fmt.Errorf(
				"job %s (%s): %w",
				jobID,
				rawJobID,
				err,
			)
		}

		job.AllocMem += allocTres.Mem
	}

	return jobEff, nil
}

// JobEffCollector collects job efficiency metrics.
type JobEffCollector struct {
	clusters map[string]string

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
	ioUsed                      *prometheus.Desc
	energyUsed                  *prometheus.Desc
	pagesUsed                   *prometheus.Desc
	vmemUsed                    *prometheus.Desc

	logger *logger.Logger
}

// NewJobEffCollector creates a new collector for job efficiency metrics.
func NewJobEffCollector(logger *logger.Logger, clusters map[string]string) *JobEffCollector {
	labels := []string{"jobid", "name", "slurm_cluster", "partition", "user", "group", "account", "state"}
	return &JobEffCollector{
		clusters: clusters,

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
			"Time from job submission to job completion in seconds.",
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

		ioUsed: prometheus.NewDesc(
			"slurm_exporter_job_eff_io_bytes_total",
			"Total filesystem I/O performed by the job in bytes.",
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

// Describe implements the prometheus.Collector interface.
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
	ch <- jc.ioUsed
	ch <- jc.energyUsed
	ch <- jc.pagesUsed
	ch <- jc.vmemUsed
}

// Collect implements the prometheus.Collector interface.
func (jc *JobEffCollector) Collect(ch chan<- prometheus.Metric) {
	for clusterLabel, cluster := range jc.clusters {

		JobEffMetrics, err := ParseJobEffMetrics(jc.logger, cluster)

		if err == nil {
			for jobid, job := range JobEffMetrics {
				labels := []string{jobid, job.Name, cluster, job.Partition, job.User, job.Group, job.Account, job.State}

				timeToScience, err := subtractTime(job.End, job.Eligible)
				if err != nil {
					jc.logger.Warn(
						"Skipping job due to invalid timestamps",
						"job_id", jobid,
						"cluster", cluster,
						"end", job.End,
						"eligible", job.Eligible,
						"err", err,
					)
					continue
				}

				timeInQueue, err := subtractTime(job.Start, job.Eligible)
				if err != nil {
					jc.logger.Warn(
						"Skipping job due to invalid timestamps",
						"job_id", jobid,
						"cluster", cluster,
						"start", job.Start,
						"eligible", job.Eligible,
						"err", err,
					)
					continue
				}
				runningToTimeToScience := job.ElapsedRaw / timeToScience
				runningToLimit := job.ElapsedRaw / job.TimelimitRaw
				cpuEfficiency := job.TresUsageInTotCPU / job.CPUTimeRaw
				memoryEfficiency := job.TresUsageInTotMem / job.AllocMem

				ch <- prometheus.MustNewConstMetric(jc.timeAllocated, prometheus.GaugeValue, job.TimelimitRaw, labels...)
				ch <- prometheus.MustNewConstMetric(jc.coresAllocated, prometheus.GaugeValue, job.AllocCPUS, labels...)
				ch <- prometheus.MustNewConstMetric(jc.memoryAllocated, prometheus.GaugeValue, job.AllocMem, labels...)
				ch <- prometheus.MustNewConstMetric(jc.cpuTime, prometheus.GaugeValue, job.CPUTimeRaw, labels...)
				ch <- prometheus.MustNewConstMetric(jc.cpuUsed, prometheus.GaugeValue, job.TresUsageInTotCPU, labels...)
				ch <- prometheus.MustNewConstMetric(jc.memUsed, prometheus.GaugeValue, job.TresUsageInTotMem, labels...)
				ch <- prometheus.MustNewConstMetric(jc.timeToScience, prometheus.GaugeValue, timeToScience, labels...)
				ch <- prometheus.MustNewConstMetric(jc.timeInQueue, prometheus.GaugeValue, timeInQueue, labels...)
				ch <- prometheus.MustNewConstMetric(jc.timeRunning, prometheus.GaugeValue, job.ElapsedRaw, labels...)
				ch <- prometheus.MustNewConstMetric(jc.runningToTimeToScienceRatio, prometheus.GaugeValue, runningToTimeToScience, labels...)
				ch <- prometheus.MustNewConstMetric(jc.runningToLimitRatio, prometheus.GaugeValue, runningToLimit, labels...)
				ch <- prometheus.MustNewConstMetric(jc.cpuEfficiency, prometheus.GaugeValue, cpuEfficiency, labels...)
				ch <- prometheus.MustNewConstMetric(jc.memoryEfficiency, prometheus.GaugeValue, memoryEfficiency, labels...)
				ch <- prometheus.MustNewConstMetric(jc.ioUsed, prometheus.GaugeValue, job.TresUsageInTotFSDisk, labels...)
				ch <- prometheus.MustNewConstMetric(jc.energyUsed, prometheus.GaugeValue, job.TresUsageInTotEnergy, labels...)
				ch <- prometheus.MustNewConstMetric(jc.pagesUsed, prometheus.GaugeValue, job.TresUsageInTotPages, labels...)
				ch <- prometheus.MustNewConstMetric(jc.vmemUsed, prometheus.GaugeValue, job.TresUsageInTotVMem, labels...)
			}
		} else {
			jc.logger.Error("Failed to parse job efficiency", "slurm_cluster", clusterLabel, "err", err)
		}
	}
}
