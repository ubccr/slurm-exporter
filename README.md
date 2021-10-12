# Slurm Exporter for Prometheus

Prometheus exporter for metrics collected from [Slurm](https://slurm.schedmd.com)
using the [REST api](https://slurm.schedmd.com/rest.html).

## Install

Download the latest release [here](https://github.com/ubccr/slurm-exporter/releases).

```
$ tar xvzf slurm-exporter-XXX.linux-amd64.tar.gz
$ sudo cp slurm-exporter-XXX.linux-amd64/slurm-exporter /usr/local/bin
```

### Create a user for running slurm-exporter 

```
$ sudo groupadd -r prometheus
$ sudo useradd -r -g prometheus -d /srv/slurmrestd -s /sbin/nologin -c 'prometheus exporter' prometheus
$ sudo mkdir /srv/slurmrestd
$ sudo chown prometheus.prometheus /srv/slurmrestd
```

### Setup and run slurmrestd 

Serveral methods exist for deploying slurmrestd and in this example we run
slurmrestd to listen on a unix domain socket using a simple shell script and
systemd unit file:

Simple startup script:

```
$ cat /srv/slurmrestd/start.sh
#!/bin/bash

rm -f /srv/slurmrestd/slurmrestd.sock
/sbin/slurmrestd unix:/srv/slurmrestd/slurmrestd.sock
```

Systemd unit file:

```
$ cat /etc/systemd/system/slurmrestd.service
[Unit]
Description=Slurm REST service
After=syslog.target network.target sssd.service

[Service]
Type=simple
User=prometheus
Group=prometheus
WorkingDirectory=/srv/slurmrestd
ExecStart=/srv/slurmrestd/start.sh
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Start slurmrestd:

```
$ sudo systemctl daemon-reload
$ sudo systemctl start slurmrestd
```

Check you can connect to slurmrestd:

```
sudo -u prometheus curl -vvv -H 'Accept: application/json' --unix-socket /srv/slurmrestd/slurmrestd.sock http:/slurm/v0.0.37/nodes
```

### Setup and run slurm-exporter

Create a systemd unit file for running the slurm-exporter:

```
$ cat /etc/systemd/system/slurm-exporter.service
[Unit]
Description=Prometheus Slurm Exporter
Wants=network-online.target
After=network-online.target slurmrestd.service

[Service]
User=prometheus
Group=prometheus
Type=simple
ExecStart=/usr/local/bin/slurm-exporter --listen-address=:8080 --unix-socket=/srv/slurmrestd/slurmrestd.sock
Restart=on-abort

[Install]
WantedBy=multi-user.target
```

Start slurm-exporter:

```
$ sudo systemctl daemon-reload
$ sudo systemctl start slurm-exporter
```

Check you can connect:

```
$ curl http://localhost:8080/metrics
```

## Configure slurm-exporter in prometheus

```yml
- targets:
  - localhost:8080
```

## See Also

This project is loosely based on [prometheus-slurm-exporter](https://github.com/vpenso/prometheus-slurm-exporter)
and exports similar metric names.

## License

SlurmExporter is released under the GPLv3 license. See the LICENSE file.
