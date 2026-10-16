package collector

import (
	"bufio"
	"bytes"
	"strings"

	"gitea.psi.ch/agosti_j/slurm_exporter/internal/logger"
	"github.com/prometheus/client_golang/prometheus"
)

// PriorityInfo holds the priority-related configuration parameters
// retrieved from the Slurm configuration.
type PriorityInfo struct {
	PriorityParameters           string
	PrioritySiteFactorParameters string
	PrioritySiteFactorPlugin     string
	PriorityDecayHalfLife        string
	PriorityCalcPeriod           string
	PriorityFavorSmall           string
	PriorityFlags                string
	PriorityMaxAge               string
	PriorityType                 string
	PriorityUsageResetPeriod     string
	PriorityWeightAge            string
	PriorityWeightAssoc          string
	PriorityWeightFairShare      string
	PriorityWeightJobSize        string
	PriorityWeightPartition      string
	PriorityWeightQOS            string
	PriorityWeightTRES           string
}

// ScontrolConfigData executes "scontrol show config" and returns
// the raw Slurm configuration output.
func ScontrolConfigData(logger *logger.Logger) ([]byte, error) {
	args := []string{"show", "config"}

	return Execute(logger, "scontrol", args)
}

// ParsePriorityInfo extracts priority-related parameters from the
// Slurm configuration output and stores them in a PriorityInfo struct.
func ParsePriorityInfo(data []byte) PriorityInfo {
	var info PriorityInfo

	scanner := bufio.NewScanner(bytes.NewReader(data))

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments.
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Split each configuration line into its key and value.
		// Ignore lines that do not contain an equals sign.
		key, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}

		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)

		// Store recognized priority parameters in the corresponding fields.
		switch key {
		case "PriorityParameters":
			info.PriorityParameters = value
		case "PrioritySiteFactorParameters":
			info.PrioritySiteFactorParameters = value
		case "PrioritySiteFactorPlugin":
			info.PrioritySiteFactorPlugin = value
		case "PriorityDecayHalfLife":
			info.PriorityDecayHalfLife = value
		case "PriorityCalcPeriod":
			info.PriorityCalcPeriod = value
		case "PriorityFavorSmall":
			info.PriorityFavorSmall = value
		case "PriorityFlags":
			info.PriorityFlags = value
		case "PriorityMaxAge":
			info.PriorityMaxAge = value
		case "PriorityType":
			info.PriorityType = value
		case "PriorityUsageResetPeriod":
			info.PriorityUsageResetPeriod = value
		case "PriorityWeightAge":
			info.PriorityWeightAge = value
		case "PriorityWeightAssoc":
			info.PriorityWeightAssoc = value
		case "PriorityWeightFairShare":
			info.PriorityWeightFairShare = value
		case "PriorityWeightJobSize":
			info.PriorityWeightJobSize = value
		case "PriorityWeightPartition":
			info.PriorityWeightPartition = value
		case "PriorityWeightQOS":
			info.PriorityWeightQOS = value
		case "PriorityWeightTRES":
			info.PriorityWeightTRES = value
		}
	}

	return info
}

// PriorityGetMetrics retrieves the Slurm configuration and parses
// its priority-related parameters into a PriorityInfo struct.
func PriorityGetMetrics(logger *logger.Logger) (PriorityInfo, error) {
	data, err := ScontrolConfigData(logger)
	if err != nil {
		return PriorityInfo{}, err
	}

	return ParsePriorityInfo(data), nil
}

// PriorityCollector implements the Prometheus Collector interface
// for exporting Slurm priority configuration parameters.
type PriorityCollector struct {
	priorityConfig *prometheus.Desc

	logger *logger.Logger
}

// NewPriorityCollector creates a PriorityCollector with a Prometheus
// metric descriptor containing all priority configuration parameters
// as labels.
func NewPriorityCollector(logger *logger.Logger) *PriorityCollector {
	labels := []string{
		"PriorityParameters",
		"PrioritySiteFactorParameters",
		"PrioritySiteFactorPlugin",
		"PriorityDecayHalfLife",
		"PriorityCalcPeriod",
		"PriorityFavorSmall",
		"PriorityFlags",
		"PriorityMaxAge",
		"PriorityType",
		"PriorityUsageResetPeriod",
		"PriorityWeightAge",
		"PriorityWeightAssoc",
		"PriorityWeightFairShare",
		"PriorityWeightJobSize",
		"PriorityWeightPartition",
		"PriorityWeightQOS",
		"PriorityWeightTRES",
	}

	return &PriorityCollector{
		priorityConfig: prometheus.NewDesc(
			"slurm_exporter_priority_config",
			"Priority parameters defined in the slurm config",
			labels, nil,
		),
		logger: logger,
	}
}

// Describe sends the collector's metric descriptor to Prometheus.
func (pc *PriorityCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- pc.priorityConfig
}

// Collect retrieves the current Slurm priority configuration and
// exports it as a Prometheus metric.
func (pc *PriorityCollector) Collect(ch chan<- prometheus.Metric) {
	m, err := PriorityGetMetrics(pc.logger)

	if err == nil {
		// Use the configuration values as labels on a gauge with value 1.
		// This exposes the current configuration without treating the
		// parameter values themselves as numeric measurements.
		labels := []string{
			m.PriorityParameters,
			m.PrioritySiteFactorParameters,
			m.PrioritySiteFactorPlugin,
			m.PriorityDecayHalfLife,
			m.PriorityCalcPeriod,
			m.PriorityFavorSmall,
			m.PriorityFlags,
			m.PriorityMaxAge,
			m.PriorityType,
			m.PriorityUsageResetPeriod,
			m.PriorityWeightAge,
			m.PriorityWeightAssoc,
			m.PriorityWeightFairShare,
			m.PriorityWeightJobSize,
			m.PriorityWeightPartition,
			m.PriorityWeightQOS,
			m.PriorityWeightTRES,
		}

		ch <- prometheus.MustNewConstMetric(
			pc.priorityConfig,
			prometheus.GaugeValue,
			1,
			labels...,
		)
	} else {
		// Log the error so configuration retrieval failures can be diagnosed.
		pc.logger.Error(
			"Failed to parse priority config metrics",
			"err", err,
		)
	}
}
