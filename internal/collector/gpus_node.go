package collector

import (
	"regexp"
	"strconv"
	"strings"

	"gitea.psi.ch/agosti_j/slurm_exporter/internal/logger"
	"github.com/prometheus/client_golang/prometheus"
)

// NodeGPUsCollector collects node-level GPU metrics
type NodeGPUsCollector struct {
	clusters map[string]string
	alloc    *prometheus.Desc
	total    *prometheus.Desc
	logger   *logger.Logger
}

// NewNodeGPUsCollector creates a new node-level GPU collector
func NewNodeGPUsCollector(logger *logger.Logger, clusters map[string]string) *NodeGPUsCollector {
	return &NodeGPUsCollector{
		clusters: clusters,
		alloc: prometheus.NewDesc(
			"slurm_exporter_node_gpus_alloc",
			"Allocated GPUs per node",
			[]string{"slurm_cluster", "node"}, nil),
		total: prometheus.NewDesc(
			"slurm_exporter_node_gpus_total",
			"Total GPUs per node",
			[]string{"slurm_cluster", "node"}, nil),
		logger: logger,
	}
}

// Describe sends metric descriptors to Prometheus
func (cc *NodeGPUsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.alloc
	ch <- cc.total
}

// Collect fetches node-level GPU metrics and sends them to Prometheus
func (cc *NodeGPUsCollector) Collect(ch chan<- prometheus.Metric) {
	for clusterLabel, clusterName := range cc.clusters {
		allocData, err := NodeAllocatedGPUsData(cc.logger, clusterName)
		if err != nil {
			cc.logger.Error("Failed to get allocated GPUs", "slurm_cluster", clusterName, "err", err)
			continue
		}
		totalData, err := NodeTotalGPUsData(cc.logger, clusterName)
		if err != nil {
			cc.logger.Error("Failed to get total GPUs", "slurm_cluster", clusterName, "err", err)
			continue
		}

		allocMap := ParseNodeGPUs(allocData)
		totalMap := ParseNodeGPUs(totalData)

		for node, alloc := range allocMap {
			total := totalMap[node]
			ch <- prometheus.MustNewConstMetric(cc.alloc, prometheus.GaugeValue, alloc, clusterLabel, node)
			ch <- prometheus.MustNewConstMetric(cc.total, prometheus.GaugeValue, total, clusterLabel, node)
		}
	}
}

// ParseNodeGPUs parses sinfo output into node → GPU count
func ParseNodeGPUs(data []byte) map[string]float64 {
	results := make(map[string]float64)
	re := regexp.MustCompile(`gpu:(\(null\)|[^:]+):([0-9]+)`)

	for _, line := range strings.Split(string(data), "\n") {
		if len(line) == 0 || !strings.Contains(line, "gpu:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		nodeName := fields[0]
		nodeTotal := 0.0
		for _, spec := range strings.Split(fields[1], ",") {
			matches := re.FindStringSubmatch(spec)
			if len(matches) > 2 {
				count, _ := strconv.ParseFloat(matches[2], 64)
				nodeTotal += count
			}
		}
		// Take maximum if multiple lines exist per node
		if existing, ok := results[nodeName]; !ok || nodeTotal > existing {
			results[nodeName] = nodeTotal
		}
	}
	return results
}

// All nodes with GPU info
func NodeTotalGPUsData(logger *logger.Logger, cluster string) ([]byte, error) {
	args := []string{"-N", "-h", "--Format=NodeList,Gres:"}
	if cluster != "" {
		args = append([]string{"-M", cluster}, args...)
	}
	return Execute(logger, "sinfo", args)
}

// Allocated GPUs per node
func NodeAllocatedGPUsData(logger *logger.Logger, cluster string) ([]byte, error) {
	args := []string{"-N", "-h", "--Format=NodeList,GresUsed:"}
	if cluster != "" {
		args = append([]string{"-M", cluster}, args...)
	}
	return Execute(logger, "sinfo", args)
}
