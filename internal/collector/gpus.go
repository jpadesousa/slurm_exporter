package collector

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"gitea.psi.ch/agosti_j/slurm_exporter/internal/logger"
)

// GPUsMetrics holds GPU utilization statistics from Slurm
type GPUsMetrics struct {
	alloc       map[string]float64 // Number of allocated GPUs
	idle        map[string]float64 // Number of idle GPUs
	other       map[string]float64 // Number of GPUs in other states (mixed, down, etc.)
	total       map[string]float64 // Total number of GPUs in the cluster
	utilization map[string]float64 // GPU utilization ratio (allocated/total)
}

// GPUsGetMetrics retrieves and parses GPU metrics from Slurm
func GPUsGetMetrics(logger *logger.Logger, cluster string) (*GPUsMetrics, error) {
	return ParseGPUsMetrics(logger, cluster)
}

// ParseAllocatedGPUs parses the output of sinfo command to count allocated GPUs
// Expected input format examples:
//   - slurm>=20.11.8: "3 gpu:2"
//   - slurm 21.08.5:  "1 gpu:(null):3(IDX:0-7)"
//   - slurm 21.08.5:  "13 gpu:A30:4(IDX:0-3),gpu:Q6K:4(IDX:0-3)"
func ParseAllocatedGPUs(data []byte) map[string]float64 {
	results := make(map[string]float64)
	sinfoLines := string(data)

	// Regex to match GPU specifications:
	// gpu:type:count or gpu:count
	re := regexp.MustCompile(`gpu:(\(null\)|[^:(]*):?([0-9]+)(\([^)]*\))?`)

	if len(sinfoLines) > 0 {
		for _, line := range strings.Split(sinfoLines, "\n") {
			if len(line) == 0 || !strings.Contains(line, "gpu:") {
				continue
			}

			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}

			numNodes, err := strconv.ParseFloat(fields[0], 64)
			if err != nil {
				continue
			}

			typeCounts := parseGPUCounts(fields[1], re)

			for gpuType, perNode := range typeCounts {
				results[gpuType] += numNodes * perNode
			}
		}
	}

	return results
}

// ParseIdleGPUs calculates idle GPUs by subtracting allocated from total GPUs
// Expected input format examples:
//   - slurm 20.11.8:  "3 gpu:4 gpu:2" (total available, allocated)
//   - slurm 21.08.5:  "1 gpu:8(S:0-1) gpu:(null):3(IDX:0-7)"
//   - slurm 21.08.5:  "13 gpu:A30:4(S:0-1),gpu:Q6K:40(S:0-1) gpu:A30:4(IDX:0-3),gpu:Q6K:4(IDX:0-3)"
func ParseIdleGPUs(data []byte) map[string]float64 {
	results := make(map[string]float64)
	sinfoLines := string(data)
	re := regexp.MustCompile(`gpu:(\(null\)|[^:(]*):?([0-9]+)(\([^)]*\))?`)
	if len(sinfoLines) > 0 {
		for _, line := range strings.Split(sinfoLines, "\n") {
			if len(line) > 0 && strings.Contains(line, "gpu:") {
				fields := strings.Fields(line)
				if len(fields) < 1 {
					continue
				}

				numNodes, _ := strconv.ParseFloat(fields[0], 64)

				switch len(fields) {
				case 1:
					continue
				case 2:
					// Two columns: nodes and total GPUs (no allocated info)
					totalGPUs := parseGPUCounts(fields[1], re)
					for gpuType, perNode := range totalGPUs {
						results[gpuType] += numNodes * perNode
					}
				default:
					// Three or more columns: nodes, total GPUs, allocated GPUs
					totalGPUs := parseGPUCounts(fields[1], re)
					allocatedGPUs := parseGPUCounts(fields[2], re)
					for gpuType, total := range totalGPUs {
						alloc := allocatedGPUs[gpuType]
						idle := total - alloc
						if idle < 0 {
							idle = 0
						}
						results[gpuType] += numNodes * idle
					}
				}
			}
		}
	}

	return results
}

// parseGPUCounts extracts the total GPU count from a GPU specification string
func parseGPUCounts(gpuSpec string, re *regexp.Regexp) map[string]float64 {
	counts := make(map[string]float64)
	for _, spec := range strings.Split(gpuSpec, ",") {
		if strings.Contains(spec, "gpu:") {
			matches := re.FindStringSubmatch(spec)
			if len(matches) > 2 {
				gpuType := matches[1]
				if gpuType == "(null)" || gpuType == "" {
					gpuType = "unknown"
				}
				gpuCount, _ := strconv.ParseFloat(matches[2], 64)
				counts[gpuType] += gpuCount
			}
		}
	}
	return counts
}

// ParseTotalGPUs parses the output of sinfo command to count total available GPUs
// Expected input format examples:
//   - slurm 20.11.8:  "3 gpu:4"
//   - slurm 21.08.5:  "1 gpu:8(S:0-1)"
//   - slurm 21.08.5:  "13 gpu:A30:4(S:0-1),gpu:Q6K:40(S:0-1)"
func ParseTotalGPUs(data []byte) map[string]float64 {
	results := make(map[string]float64)
	sinfoLines := string(data)
	re := regexp.MustCompile(`gpu:(\(null\)|[^:(]*):?([0-9]+)(\([^)]*\))?`)

	if len(sinfoLines) > 0 {
		for _, line := range strings.Split(sinfoLines, "\n") {
			if len(line) > 0 && strings.Contains(line, "gpu:") {
				fields := strings.Fields(line)
				if len(fields) < 2 {
					continue
				}
				numNodes, _ := strconv.ParseFloat(fields[0], 64)
				typeCounts := parseGPUCounts(fields[1], re)
				for gpuType, perNode := range typeCounts {
					results[gpuType] += numNodes * perNode
				}
			}
		}
	}

	return results
}

// ParseGPUsMetrics collects and parses all GPU metrics from Slurm
func ParseGPUsMetrics(logger *logger.Logger, cluster string) (*GPUsMetrics, error) {

	gm := GPUsMetrics{
		alloc:       make(map[string]float64),
		idle:        make(map[string]float64),
		other:       make(map[string]float64),
		total:       make(map[string]float64),
		utilization: make(map[string]float64),
	}

	// Get total GPU count
	totalGPUsData, err := TotalGPUsData(logger, cluster)
	if err != nil {
		return nil, err
	}
	totalGPUs := ParseTotalGPUs(totalGPUsData)

	// Get allocated GPU count
	allocatedGPUsData, err := AllocatedGPUsData(logger, cluster)
	if err != nil {
		return nil, err
	}
	allocatedGPUs := ParseAllocatedGPUs(allocatedGPUsData)

	// Get idle GPU count
	idleGPUsData, err := IdleGPUsData(logger, cluster)
	if err != nil {
		return nil, err
	}
	idleGPUs := ParseIdleGPUs(idleGPUsData)

	for gpuType, totalCount := range totalGPUs {
		allocCount := allocatedGPUs[gpuType]
		idleCount := idleGPUs[gpuType]
		// Calculate other GPUs (DRAIN, DOWN, RESERVED, MAINTENANCE, etc.).
		// Clamp to zero because total/allocated/idle come from three separate
		// sinfo invocations and cluster state can change between them, producing
		// a transient negative result (see issue #16). The Debug log gives a
		// trail if operators ever need to investigate suspected miscounting.
		otherCount := totalCount - allocCount - idleCount
		if otherCount < 0 {
			logger.Debug("gpus collector: clamped negative 'other' to zero (likely sinfo timing race)",
				"total", totalCount, "alloc", allocCount, "idle", idleCount, "computed_other", otherCount, "gpu_type", gpuType)
			otherCount = 0
		}

		gm.alloc[gpuType] = allocCount
		gm.idle[gpuType] = idleCount
		gm.other[gpuType] = otherCount
		gm.total[gpuType] = totalCount

		// Calculate utilization ratio
		if totalCount > 0 {
			gm.utilization[gpuType] = allocCount / totalCount
		}
	}
	return &gm, nil
}

// AllocatedGPUsData executes sinfo command to get allocated GPU information
func AllocatedGPUsData(logger *logger.Logger, cluster string) ([]byte, error) {
	args := []string{"-a", "-h", "--Format=Nodes: ,GresUsed:", "--state=allocated"}
	if cluster != "" {
		args = append([]string{"-M", cluster}, args...)
	}
	return Execute(logger, "sinfo", args)
}

// IdleGPUsData executes sinfo command to get idle and allocated GPU information.
//
// Trailing ":" on each field forces variable column widths; fixed widths
// (was Gres:50, GresUsed:50) silently truncate rich GRES specs on busy GPU
// nodes (multi-type GPUs, MIG slices), causing wrong GPU counts.
// See https://gitea.psi.ch/agosti_j/slurm_exporter/issues/10.
func IdleGPUsData(logger *logger.Logger, cluster string) ([]byte, error) {
	args := []string{"-a", "-h", "--Format=Nodes: ,Gres: ,GresUsed:", "--state=idle,allocated"}
	if cluster != "" {
		args = append([]string{"-M", cluster}, args...)
	}
	return Execute(logger, "sinfo", args)
}

// TotalGPUsData executes sinfo command to get total GPU information.
// See IdleGPUsData for the rationale behind the variable-width format.
func TotalGPUsData(logger *logger.Logger, cluster string) ([]byte, error) {
	args := []string{"-a", "-h", "--Format=Nodes: ,Gres:"}
	if cluster != "" {
		args = append([]string{"-M", cluster}, args...)
	}
	return Execute(logger, "sinfo", args)
}

// NewGPUsCollector creates a new GPU metrics collector
func NewGPUsCollector(logger *logger.Logger, clusters map[string]string) *GPUsCollector {
	return &GPUsCollector{
		clusters:    clusters,
		alloc:       prometheus.NewDesc("slurm_exporter_gpus_alloc", "Allocated GPUs", []string{"slurm_cluster", "type"}, nil),
		idle:        prometheus.NewDesc("slurm_exporter_gpus_idle", "Idle GPUs", []string{"slurm_cluster", "type"}, nil),
		other:       prometheus.NewDesc("slurm_exporter_gpus_other", "Other GPUs", []string{"slurm_cluster", "type"}, nil),
		total:       prometheus.NewDesc("slurm_exporter_gpus_total", "Total GPUs", []string{"slurm_cluster", "type"}, nil),
		utilization: prometheus.NewDesc("slurm_exporter_gpus_utilization", "Total GPU utilization", []string{"slurm_cluster", "type"}, nil),
		logger:      logger,
	}
}

// GPUsCollector implements the Prometheus Collector interface for GPU metrics
type GPUsCollector struct {
	clusters    map[string]string
	alloc       *prometheus.Desc
	idle        *prometheus.Desc
	other       *prometheus.Desc
	total       *prometheus.Desc
	utilization *prometheus.Desc
	logger      *logger.Logger
}

// Describe sends the descriptors of each metric over to the provided channel
func (cc *GPUsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.alloc
	ch <- cc.idle
	ch <- cc.other
	ch <- cc.total
	ch <- cc.utilization
}

// Collect fetches the GPU metrics from Slurm and sends them to Prometheus
func (cc *GPUsCollector) Collect(ch chan<- prometheus.Metric) {
	for clusterLabel, clusterName := range cc.clusters {
		metrics, err := GPUsGetMetrics(cc.logger, clusterName)
		if err != nil {
			cc.logger.Error("Failed to get GPU metrics", "slurm_cluster", clusterName, "err", err)
			return
		}

		for gpuType := range metrics.total {
			ch <- prometheus.MustNewConstMetric(cc.alloc, prometheus.GaugeValue, metrics.alloc[gpuType], clusterLabel, gpuType)
			ch <- prometheus.MustNewConstMetric(cc.idle, prometheus.GaugeValue, metrics.idle[gpuType], clusterLabel, gpuType)
			ch <- prometheus.MustNewConstMetric(cc.other, prometheus.GaugeValue, metrics.other[gpuType], clusterLabel, gpuType)
			ch <- prometheus.MustNewConstMetric(cc.total, prometheus.GaugeValue, metrics.total[gpuType], clusterLabel, gpuType)
			ch <- prometheus.MustNewConstMetric(cc.utilization, prometheus.GaugeValue, metrics.utilization[gpuType], clusterLabel, gpuType)
		}
	}
}
