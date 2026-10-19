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

type SprioInfo struct {
	JobID           string
	Partition       string
	User            string
	Account         string
	Priority        float64
	Site            float64
	Age             float64
	Assoc           float64
	FairShare       float64
	JobSize         float64
	PartitionWeight float64
	QOSName         string
	QOS             float64
	Nice            float64
	TRE             float64
}

func SprioData(logger *logger.Logger, cluster string) ([]byte, error) {
	args := []string{"-h", "-o", "%i|%r|%u|%o|%Y|%S|%A|%B|%F|%J|%P|%n|%Q|%N|%T"}

	if cluster != "" {
		args = append([]string{"-M", cluster}, args...)
	}

	return Execute(logger, "sprio", args)
}

func splitSprioFields(line string, fields *[15]string) bool {
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

func ParseSprioMetrics(logger *logger.Logger, cluster string) (map[string]*SprioInfo, error) {
	sprio := make(map[string]*SprioInfo)

	sprioData, err := SprioData(logger, cluster)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(bytes.NewReader(sprioData))

	for scanner.Scan() {
		line := scanner.Text()

		var parts [15]string
		if !splitSprioFields(line, &parts) {
			continue
		}

		jobID := parts[0]

		var priority float64
		if parts[4] != "" {
			priority, err = strconv.ParseFloat(parts[4], 64)
			if err != nil {
				logger.Warn(
					"invalid Priority value",
					"jobID", jobID,
					"value", parts[4],
					"error", err,
				)
				continue
			}
		}

		var site float64
		if parts[5] != "" {
			site, err = strconv.ParseFloat(parts[5], 64)
			if err != nil {
				logger.Warn(
					"invalid Site value",
					"jobID", jobID,
					"value", parts[5],
					"error", err,
				)
				continue
			}
		}

		var age float64
		if parts[6] != "" {
			age, err = strconv.ParseFloat(parts[6], 64)
			if err != nil {
				logger.Warn(
					"invalid Age value",
					"jobID", jobID,
					"value", parts[6],
					"error", err,
				)
				continue
			}
		}

		var assoc float64
		if parts[7] != "" {
			assoc, err = strconv.ParseFloat(parts[7], 64)
			if err != nil {
				logger.Warn(
					"invalid Assoc value",
					"jobID", jobID,
					"value", parts[7],
					"error", err,
				)
				continue
			}
		}

		var fairshare float64
		if parts[8] != "" {
			fairshare, err = strconv.ParseFloat(parts[8], 64)
			if err != nil {
				logger.Warn(
					"invalid Fairshare value",
					"jobID", jobID,
					"value", parts[8],
					"error", err,
				)
				continue
			}
		}

		var jobsize float64
		if parts[9] != "" {
			jobsize, err = strconv.ParseFloat(parts[9], 64)
			if err != nil {
				logger.Warn(
					"invalid Job Size value",
					"jobID", jobID,
					"value", parts[9],
					"error", err,
				)
				continue
			}
		}

		var partitionWeight float64
		if parts[10] != "" {
			partitionWeight, err = strconv.ParseFloat(parts[10], 64)
			if err != nil {
				logger.Warn(
					"invalid PartitionWeight value",
					"jobID", jobID,
					"value", parts[10],
					"error", err,
				)
				continue
			}
		}

		var qos float64
		if parts[12] != "" {
			qos, err = strconv.ParseFloat(parts[12], 64)
			if err != nil {
				logger.Warn(
					"invalid QOS value",
					"jobID", jobID,
					"value", parts[12],
					"error", err,
				)
				continue
			}
		}

		var nice float64
		if parts[13] != "" {
			nice, err = strconv.ParseFloat(parts[13], 64)
			if err != nil {
				logger.Warn(
					"invalid Nice value",
					"jobID", jobID,
					"value", parts[13],
					"error", err,
				)
				continue
			}
		}

		var tre float64
		if parts[14] != "" {
			tre, err = strconv.ParseFloat(parts[14], 64)
			if err != nil {
				logger.Warn(
					"invalid TRE value",
					"jobID", jobID,
					"value", parts[14],
					"error", err,
				)
				continue
			}
		}

		sprio[jobID] = &SprioInfo{
			JobID:           jobID,
			Partition:       parts[1],
			User:            parts[2],
			Account:         parts[3],
			Priority:        priority,
			Site:            site,
			Age:             age,
			Assoc:           assoc,
			FairShare:       fairshare,
			JobSize:         jobsize,
			PartitionWeight: partitionWeight,
			QOSName:         parts[11],
			QOS:             qos,
			Nice:            nice,
			TRE:             tre,
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning sprio output: %w", err)
	}

	return sprio, nil
}

type SprioCollector struct {
	clusters        map[string]string
	priority        *prometheus.Desc
	site            *prometheus.Desc
	age             *prometheus.Desc
	assoc           *prometheus.Desc
	fairshare       *prometheus.Desc
	jobSize         *prometheus.Desc
	partitionWeight *prometheus.Desc
	qos             *prometheus.Desc
	nice            *prometheus.Desc
	tre             *prometheus.Desc
	logger          *logger.Logger
}

func NewSprioCollector(logger *logger.Logger, clusters map[string]string) *SprioCollector {
	labels := []string{"jobid", "partition", "user", "account", "QOS name", "cluster"}

	return &SprioCollector{
		clusters: clusters,

		priority: prometheus.NewDesc(
			"slurm_exporter_sprio_job_priority",
			"Job priority value.",
			labels, nil,
		),
		site: prometheus.NewDesc(
			"slurm_exporter_sprio_job_site",
			"Job site value.",
			labels, nil,
		),
		age: prometheus.NewDesc(
			"slurm_exporter_sprio_job_age",
			"Job age value.",
			labels, nil,
		),
		assoc: prometheus.NewDesc(
			"slurm_exporter_sprio_job_assoc",
			"Job assoc value.",
			labels, nil,
		),
		fairshare: prometheus.NewDesc(
			"slurm_exporter_sprio_job_fairshare",
			"Job fairshare value.",
			labels, nil,
		),
		jobSize: prometheus.NewDesc(
			"slurm_exporter_sprio_job_jobsize",
			"Job jobsize value.",
			labels, nil,
		),
		partitionWeight: prometheus.NewDesc(
			"slurm_exporter_sprio_job_partitionweight",
			"Partition weigth value.",
			labels, nil,
		),
		qos: prometheus.NewDesc(
			"slurm_exporter_sprio_job_qos",
			"Job QOS value.",
			labels, nil,
		),
		nice: prometheus.NewDesc(
			"slurm_exporter_sprio_job_nice",
			"Job nice value.",
			labels, nil,
		),
		tre: prometheus.NewDesc(
			"slurm_exporter_sprio_job_tre",
			"Job TRE value.",
			labels, nil,
		),
		logger: logger,
	}
}

func (jc *SprioCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- jc.priority
	ch <- jc.site
	ch <- jc.age
	ch <- jc.assoc
	ch <- jc.fairshare
	ch <- jc.jobSize
	ch <- jc.partitionWeight
	ch <- jc.qos
	ch <- jc.nice
	ch <- jc.tre
}

func (sc *SprioCollector) Collect(ch chan<- prometheus.Metric) {

	for clusterLabel, clusterName := range sc.clusters {
		metrics, err := ParseSprioMetrics(sc.logger, clusterName)
		if err != nil {
			sc.logger.Error("Failed to get Sprio metrics", "slurm_cluster", clusterName, "err", err)
			return
		}

		for jobid, job := range metrics {
			labels := []string{
				jobid,
				job.Partition,
				job.User,
				job.Account,
				job.QOSName,
				clusterLabel,
			}

			ch <- prometheus.MustNewConstMetric(sc.priority, prometheus.GaugeValue, job.Priority, labels...)
			ch <- prometheus.MustNewConstMetric(sc.site, prometheus.GaugeValue, job.Site, labels...)
			ch <- prometheus.MustNewConstMetric(sc.age, prometheus.GaugeValue, job.Age, labels...)
			ch <- prometheus.MustNewConstMetric(sc.assoc, prometheus.GaugeValue, job.Assoc, labels...)
			ch <- prometheus.MustNewConstMetric(sc.fairshare, prometheus.GaugeValue, job.FairShare, labels...)
			ch <- prometheus.MustNewConstMetric(sc.jobSize, prometheus.GaugeValue, job.JobSize, labels...)
			ch <- prometheus.MustNewConstMetric(sc.partitionWeight, prometheus.GaugeValue, job.PartitionWeight, labels...)
			ch <- prometheus.MustNewConstMetric(sc.qos, prometheus.GaugeValue, job.QOS, labels...)
			ch <- prometheus.MustNewConstMetric(sc.nice, prometheus.GaugeValue, job.Nice, labels...)
			ch <- prometheus.MustNewConstMetric(sc.tre, prometheus.GaugeValue, job.TRE, labels...)
		}
	}

}
