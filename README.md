[![Go](https://github.com/opensvc/oc3/actions/workflows/go.yml/badge.svg)](https://github.com/opensvc/oc3/actions/workflows/go.yml)

## configuration example
```yaml
db:
  password: xxxxxxx
  log:
    level: info
    slow_query_threshold: 500ms

feeder:
  addr: 127.0.0.1:8080
  pprof:
    ux.enable: true
    net.enable: true
  metrics:
    enable: true
  ui:
    enable: true

server:
  tx: true
  addr: 127.0.0.1:8081
  pprof:
    ux.enable: true
    net.enable: true
  metrics:
    enable: true
  ui:
    enable: true

scheduler:
  pprof:
    ux.socket: /oc3/scheduler.pprof
    ux.enable: true
    net.enable: true
  metrics:
    enable: true

messenger:
  url: http://0.0.0.0:8889
  key: magix123
  pprof:
    ux:
      enable: true
    net:
      enable: true
  metrics:
    enable: true

worker.slow:
  addr: 127.0.0.1:8100
  tx: true
  pprof:
    ux.enable: true
    net.enable: true
  runners: 3
  metrics:
    enable: true
  queues:
    - "daemon_status"
    - "daemon_ping"

worker.fast:
  addr: 127.0.0.1:8101
  tx: true
  pprof:
    ux.enable: true
    net.enable: true
  runners: 3
  metrics:
    enable: true
  queues:
    - "instance_resource_info"
    - "instance_status"
    - "instance_action"
    - "node_disk"
    - "object_config"
    - "system"
```