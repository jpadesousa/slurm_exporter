# Slurm Exporter

A Prometheus collector and exporter for [Slurm](https://slurm.schedmd.com/), designed to provide comprehensive monitoring and observability for HPC clusters.

This project is a fork of [SckyzO/slurm_exporter](https://github.com/SckyzO/slurm_exporter), extended with additional features and metrics for deeper insight into cluster health, workload performance, resource utilization, and job scheduling.

Key features include:

- **Multi-cluster metrics** — monitor and analyze metrics independently for each Slurm cluster.
- **Job efficiency metrics** — assess job performance and resource utilization.
- **Per-job metrics** — gain detailed visibility into individual jobs.
- **QoS and priority metrics** — monitor Quality of Service settings and job scheduling priorities.
- **`sprio` metrics** — inspect job priority components and understand scheduling decisions.
- **Comprehensive Slurm metrics** — track nodes, partitions, jobs, CPU and GPU resources, scheduler activity, fairshare, reservations, and licenses.

The project includes seven ready-to-use Grafana dashboards:

- `slurm-cluster-status.json` — cluster health and overall status.
- `slurm-exporter-perf.json` — exporter performance and metrics collection.
- `slurm-health.json` — cluster health and operational indicators.
- `slurm-job-efficiency.json` — job efficiency and resource utilization.
- `slurm-job-priority-qos.json` — job priorities, QoS, and scheduling configuration.
- `slurm-job-status.json` — job states and workload status.
- `slurm-scheduler.json` — scheduler activity and behavior.

Together, these dashboards provide an overview of cluster operations, job lifecycles, resource efficiency, and scheduling behavior, including visibility into the additional metrics introduced by this fork.

**Cluster-specific configuration:** The dashboards are initially adapted to the [PSI Merlin cluster](https://hpce.pages.psi.ch). They may require adjustments to match the configuration and specifications of other Slurm clusters, including cluster names, partitions, QoS policies, and available metrics.