module github.com/ubccr/slurm-exporter

go 1.14

require (
	github.com/prometheus/client_golang v1.6.0
	github.com/sirupsen/logrus v1.4.2
	github.com/ubccr/go-slurmrest v1.0.0
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
)

replace github.com/ubccr/go-slurmrest => /home/centos/projects/go/src/github.com/ubccr/go-slurmrest
