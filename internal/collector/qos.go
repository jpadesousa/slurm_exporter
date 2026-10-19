package collector

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/jpadesousa/slurm_exporter/internal/logger"
	"github.com/prometheus/client_golang/prometheus"
)

// QosInfo holds the raw QOS configuration and its parsed resource limits.
// MaxTRES contains per-job limits, while MaxTRESPU contains per-user limits.
type QosInfo struct {
	Name      string
	MaxTRES   string
	MaxTRESPU string

	MaxTRESCPU     float64
	MaxTRESMem     float64
	MaxTRESNode    float64
	MaxTRESGRESGPU float64

	MaxTRESPUCPU     float64
	MaxTRESPUMem     float64
	MaxTRESPUNode    float64
	MaxTRESPUGRESGPU float64
}

// QosData retrieves QOS names and their per-job and per-user TRES limits
// from Slurm using sacctmgr's pipe-delimited output format.
func QosData(logger *logger.Logger) ([]byte, error) {
	args := []string{"show", "qos", "-P", "-n", "format=Name,MaxTRES,MaxTRESPU"}

	return Execute(logger, "sacctmgr", args)
}

// MaxTRES stores parsed resource limits for CPUs, memory, nodes, and GPUs.
// Resource values are represented as float64 for Prometheus metric compatibility.
type MaxTRES struct {
	CPU     float64
	Mem     float64
	Node    float64
	GRESGPU float64
}

// parseTRES parses a comma-separated Slurm TRES string into resource limits.
//
// Recognized resources are cpu, mem, node, and gres/gpu. Unknown resources
// and malformed entries without an '=' separator are ignored.
func parseTRES(s string) (MaxTRES, error) {
	var result MaxTRES

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

		case "gres/gpu":
			v, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return result, fmt.Errorf("invalid gres/gpu %q: %w", value, err)
			}
			result.GRESGPU += v
		}
	}

	return result, nil
}

// splitQosFields splits a pipe-delimited QOS record into exactly three fields:
// the QOS name, MaxTRES, and MaxTRESPU.
//
// It returns false if the line does not contain the expected field structure.
func splitQosFields(line string, fields *[3]string) bool {
	for i := range fields {
		field, rest, found := strings.Cut(line, "|")
		fields[i] = field

		if !found {
			// The final field does not need a trailing pipe delimiter.
			return i == len(fields)-1
		}

		line = rest
	}

	return true
}

// ParseQosMetrics retrieves QOS configuration data and converts its resource
// limits into numeric values suitable for Prometheus metrics.
//
// Invalid resource limits are logged as warnings. Scanner errors are returned
// to the caller.
func ParseQosMetrics(logger *logger.Logger) (map[string]*QosInfo, error) {
	qos := make(map[string]*QosInfo)

	qosData, err := QosData(logger)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(bytes.NewReader(qosData))

	// Increase the scanner's maximum token size to accommodate long TRES records.
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()

		var parts [3]string
		if !splitQosFields(line, &parts) {
			continue
		}

		name := parts[0]

		qos[name] = &QosInfo{
			Name: name,
		}

		// Parse the maximum resource limits applicable to a single job.
		TresMax, err := parseTRES(parts[1])
		if err != nil {
			logger.Warn(
				"invalid MaxTRES",
				"name", name,
				"value", parts[1],
				"error", err,
			)
			continue
		}

		qos[name].MaxTRESCPU += TresMax.CPU
		qos[name].MaxTRESMem += TresMax.Mem
		qos[name].MaxTRESNode += TresMax.Node
		qos[name].MaxTRESGRESGPU += TresMax.GRESGPU

		// Parse the maximum resource limits applicable to each user.
		TresPUMax, err := parseTRES(parts[2])
		if err != nil {
			logger.Warn(
				"invalid MaxTRESPU",
				"name", name,
				"value", parts[2],
				"error", err,
			)
			continue
		}

		qos[name].MaxTRESPUCPU += TresPUMax.CPU
		qos[name].MaxTRESPUMem += TresPUMax.Mem
		qos[name].MaxTRESPUNode += TresPUMax.Node
		qos[name].MaxTRESPUGRESGPU += TresPUMax.GRESGPU
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning sacctmgr output: %w", err)
	}

	return qos, nil
}

// QosCollector exports Slurm QOS resource limits as Prometheus gauge metrics.
type QosCollector struct {
	clusters map[string]string

	maxCpuPerJob  *prometheus.Desc
	maxMemPerJob  *prometheus.Desc
	maxNodePerJob *prometheus.Desc
	maxGpuPerJob  *prometheus.Desc

	maxCpuPerUser  *prometheus.Desc
	maxMemPerUser  *prometheus.Desc
	maxNodePerUser *prometheus.Desc
	maxGpuPerUser  *prometheus.Desc

	logger *logger.Logger
}

// NewQosCollector creates a QOS collector and defines its Prometheus metrics.
//
// Each metric is labeled with the QOS name.
func NewQosCollector(logger *logger.Logger) *QosCollector {
	labels := []string{"name"}

	return &QosCollector{
		maxCpuPerJob: prometheus.NewDesc(
			"slurm_exporter_qos_max_cpus_per_job",
			"Maximum number of CPUs allowed per job.",
			labels, nil,
		),
		maxMemPerJob: prometheus.NewDesc(
			"slurm_exporter_qos_max_mem_per_job_bytes",
			"Maximum memory allowed per job in bytes.",
			labels, nil,
		),
		maxNodePerJob: prometheus.NewDesc(
			"slurm_exporter_qos_max_nodes_per_job",
			"Maximum number of nodes allowed per job.",
			labels, nil,
		),
		maxGpuPerJob: prometheus.NewDesc(
			"slurm_exporter_qos_max_gpus_per_job",
			"Maximum number of GPUs allowed per job.",
			labels, nil,
		),

		maxCpuPerUser: prometheus.NewDesc(
			"slurm_exporter_qos_max_cpus_per_user",
			"Maximum number of CPUs allowed per user.",
			labels, nil,
		),
		maxMemPerUser: prometheus.NewDesc(
			"slurm_exporter_qos_max_mem_per_user_bytes",
			"Maximum memory allowed per user in bytes.",
			labels, nil,
		),
		maxNodePerUser: prometheus.NewDesc(
			"slurm_exporter_qos_max_nodes_per_user",
			"Maximum number of nodes allowed per user.",
			labels, nil,
		),
		maxGpuPerUser: prometheus.NewDesc(
			"slurm_exporter_qos_max_gpus_per_user",
			"Maximum number of GPUs allowed per user.",
			labels, nil,
		),
		logger: logger,
	}
}

// Describe implements prometheus.Collector and sends all metric descriptors
// to the provided channel.
func (jc *QosCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- jc.maxCpuPerJob
	ch <- jc.maxMemPerJob
	ch <- jc.maxNodePerJob
	ch <- jc.maxGpuPerJob
	ch <- jc.maxCpuPerUser
	ch <- jc.maxMemPerUser
	ch <- jc.maxNodePerUser
	ch <- jc.maxGpuPerUser
}

// Collect implements prometheus.Collector and exports the current resource
// limits for each QOS as gauge metrics.
func (qc *QosCollector) Collect(ch chan<- prometheus.Metric) {
	m, err := ParseQosMetrics(qc.logger)

	if err != nil {
		qc.logger.Error("Failed to get QOS metrics", "err", err)
		return
	}

	for QosType := range m {
		// Export resource limits that apply to individual jobs.
		ch <- prometheus.MustNewConstMetric(qc.maxCpuPerJob, prometheus.GaugeValue, m[QosType].MaxTRESCPU, QosType)
		ch <- prometheus.MustNewConstMetric(qc.maxMemPerJob, prometheus.GaugeValue, m[QosType].MaxTRESMem, QosType)
		ch <- prometheus.MustNewConstMetric(qc.maxNodePerJob, prometheus.GaugeValue, m[QosType].MaxTRESNode, QosType)
		ch <- prometheus.MustNewConstMetric(qc.maxGpuPerJob, prometheus.GaugeValue, m[QosType].MaxTRESGRESGPU, QosType)

		// Export resource limits that apply to individual users.
		ch <- prometheus.MustNewConstMetric(qc.maxCpuPerUser, prometheus.GaugeValue, m[QosType].MaxTRESPUCPU, QosType)
		ch <- prometheus.MustNewConstMetric(qc.maxMemPerUser, prometheus.GaugeValue, m[QosType].MaxTRESPUMem, QosType)
		ch <- prometheus.MustNewConstMetric(qc.maxNodePerUser, prometheus.GaugeValue, m[QosType].MaxTRESPUNode, QosType)
		ch <- prometheus.MustNewConstMetric(qc.maxGpuPerUser, prometheus.GaugeValue, m[QosType].MaxTRESPUGRESGPU, QosType)
	}
}
