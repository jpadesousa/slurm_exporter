package collector

// JOBID
// PARTITION
// USER
// ACCOUNT
// PRIORITY
// SITE
// AGE
// ASSOC
// FAIRSHARE
// JOBSIZE
// PARTITION
// QOSNAME
// QOS
// NICE
// TRES

// SprioWeightsInfo holds information about the weights given to each SLURM job.
type SprioWeightsInfo struct {
	JobID     string
	Partition string
	User      string
	Cluster   string
	Account   string
	Priority  string
	Site      string
	Age       string
	Assoc     string
	Fairshare string
	JobSize   string
	QOSName   string
	QOS       string
	NICE      string
	TRES      string
}
