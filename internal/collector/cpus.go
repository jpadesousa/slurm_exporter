package collector

import (
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"gitea.psi.ch/agosti_j/slurm_exporter/internal/logger"
)

type CPUsMetrics struct {
	alloc float64
	idle  float64
	other float64
	total float64
}

func CPUsGetMetrics(logger *logger.Logger, clusters map[string]string) (map[string]*CPUsMetrics, error) {
	results := make(map[string]*CPUsMetrics)
	for label, clusterName := range clusters {
		data, err := CPUsData(logger, clusterName)
		if err != nil {
			return nil, err
		}
		results[label] = ParseCPUsMetrics(data)
	}
	return results, nil
}

/*
ParseCPUsMetrics parses the output of the sinfo command for CPU metrics.
Expected input format: "allocated/idle/other/total".
*/
func ParseCPUsMetrics(input []byte) *CPUsMetrics {
	var cm CPUsMetrics
	if strings.Contains(string(input), "/") {
		splitted := strings.Split(strings.TrimSpace(string(input)), "/")
		if len(splitted) < 4 {
			return &cm
		}
		cm.alloc, _ = strconv.ParseFloat(splitted[0], 64)
		cm.idle, _ = strconv.ParseFloat(splitted[1], 64)
		cm.other, _ = strconv.ParseFloat(splitted[2], 64)
		cm.total, _ = strconv.ParseFloat(splitted[3], 64)
	}
	return &cm
}

/*
CPUsData executes the sinfo command to retrieve CPU information.
Expected sinfo output format: "%C" (allocated/idle/other/total CPUs).
*/
func CPUsData(logger *logger.Logger, cluster string) ([]byte, error) {
	args := []string{"-a", "-h", "-o", "%C"}
	if cluster != "" {
		args = append([]string{"-M", cluster}, args...)
	}
	return Execute(logger, "sinfo", args)
}

func NewCPUsCollector(logger *logger.Logger, clusters map[string]string) *CPUsCollector {
	return &CPUsCollector{
		clusters: clusters,
		alloc:    prometheus.NewDesc("slurm_exporter_cpus_alloc", "Allocated CPUs", []string{"slurm_cluster"}, nil),
		idle:     prometheus.NewDesc("slurm_exporter_cpus_idle", "Idle CPUs", []string{"slurm_cluster"}, nil),
		other:    prometheus.NewDesc("slurm_exporter_cpus_other", "Mix CPUs", []string{"slurm_cluster"}, nil),
		total:    prometheus.NewDesc("slurm_exporter_cpus_total", "Total CPUs", []string{"slurm_cluster"}, nil),
		logger:   logger,
	}
}

type CPUsCollector struct {
	clusters map[string]string
	alloc    *prometheus.Desc
	idle     *prometheus.Desc
	other    *prometheus.Desc
	total    *prometheus.Desc
	logger   *logger.Logger
}

func (cc *CPUsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.alloc
	ch <- cc.idle
	ch <- cc.other
	ch <- cc.total
}

func (cc *CPUsCollector) Collect(ch chan<- prometheus.Metric) {
	metrics, err := CPUsGetMetrics(cc.logger, cc.clusters)
	if err != nil {
		cc.logger.Error("Failed to get CPUs metrics", "err", err)
		return
	}

	for clusterLabel, cm := range metrics {
		ch <- prometheus.MustNewConstMetric(cc.alloc, prometheus.GaugeValue, cm.alloc, clusterLabel)
		ch <- prometheus.MustNewConstMetric(cc.idle, prometheus.GaugeValue, cm.idle, clusterLabel)
		ch <- prometheus.MustNewConstMetric(cc.other, prometheus.GaugeValue, cm.other, clusterLabel)
		ch <- prometheus.MustNewConstMetric(cc.total, prometheus.GaugeValue, cm.total, clusterLabel)
	}
}
