package main

import (
	"fmt"
	"math/rand/v2"
	"strings"
)

// The realistic catalog (-catalog realistic): the fleet of a mid-sized company
// rather than numbered clusters, for the screenshots of the documentation. Its
// apps, teams, environments, datacenters, servers and services read as a
// production would: its nodes and clusters are named by their datacenter
// (pa1-billing-db01, ly2-kafka01), its services by what they run
// (prd/svc/billing-pgsql). The prefix names its arrays and switches and seeds
// its ids; the counts come from the catalog, and teardown lists its nodes by
// name.

type (
	// hwProfile is what the asset of a node says of its hardware and its system.
	hwProfile struct {
		manufacturer, model, cpuModel string
		cores, threads, memMB         int
		osVendor, osRelease, osKernel string
		pkgType                       string // rpm or deb
	}

	// site is a datacenter: the location of the nodes.
	site struct {
		code, country, city, building, room string
		net                                 int // second byte of the addresses of its nodes
	}

	catalogService struct {
		name, ns, topology string
		// kinds are the resources of the service, in order: ip, vg, fs,
		// container, app, sync.
		kinds []string
		// image is the image of its containers.
		image string
	}

	catalogCluster struct {
		name, app, team, env string
		site                 *site
		hw                   *hwProfile
		// host is the base of the names of its nodes: pa1-billing-db01.
		host string
		// role chooses the packages of its nodes: db, app, web…
		role     string
		nodes    int
		services []catalogService
	}

	// nodeProfile is what a node of the catalog adds to the generic one.
	nodeProfile struct {
		env, team, role string
		site            *site
		hw              *hwProfile
		rack            string
	}
)

var (
	sitePA1 = &site{code: "PA1", country: "France", city: "Paris", building: "PA1-B", room: "S2", net: 21}
	siteLY2 = &site{code: "LY2", country: "France", city: "Lyon", building: "LY2-A", room: "S1", net: 42}

	hwDB = &hwProfile{manufacturer: "Dell Inc.", model: "PowerEdge R760", cpuModel: "Intel(R) Xeon(R) Gold 6430 @ 2.10GHz",
		cores: 32, threads: 64, memMB: 524288, osVendor: "Red Hat", osRelease: "9.4", osKernel: "5.14.0-427.13.1.el9_4.x86_64", pkgType: "rpm"}
	hwSAP = &hwProfile{manufacturer: "HPE", model: "ProLiant DL380 Gen11", cpuModel: "Intel(R) Xeon(R) Platinum 8462Y+ @ 2.80GHz",
		cores: 64, threads: 128, memMB: 1572864, osVendor: "SUSE", osRelease: "15-SP6", osKernel: "6.4.0-150600.23.25-default", pkgType: "rpm"}
	hwData = &hwProfile{manufacturer: "Lenovo", model: "ThinkSystem SR650 V3", cpuModel: "Intel(R) Xeon(R) Gold 5418Y @ 2.00GHz",
		cores: 24, threads: 48, memMB: 262144, osVendor: "Ubuntu", osRelease: "24.04", osKernel: "6.8.0-48-generic", pkgType: "deb"}
	hwVM = &hwProfile{manufacturer: "VMware, Inc.", model: "VMware20,1", cpuModel: "Intel(R) Xeon(R) Gold 6338 CPU @ 2.00GHz",
		cores: 8, threads: 8, memMB: 32768, osVendor: "Red Hat", osRelease: "9.4", osKernel: "5.14.0-427.13.1.el9_4.x86_64", pkgType: "rpm"}
	hwVMSmall = &hwProfile{manufacturer: "VMware, Inc.", model: "VMware20,1", cpuModel: "Intel(R) Xeon(R) Gold 6338 CPU @ 2.00GHz",
		cores: 4, threads: 4, memMB: 16384, osVendor: "Ubuntu", osRelease: "24.04", osKernel: "6.8.0-48-generic", pkgType: "deb"}
)

// The resources of the services by profile.
var (
	kindsDB     = []string{"ip", "vg", "fs", "fs", "app", "sync"}
	kindsApp    = []string{"ip", "container", "container", "app"}
	kindsWeb    = []string{"ip", "container"}
	kindsSAP    = []string{"ip", "vg", "fs", "fs", "fs", "app"}
	kindsData   = []string{"ip", "vg", "fs", "container"}
	kindsInfra  = []string{"ip", "app"}
	kindsSimple = []string{"container"}
)

var realisticCatalog = []catalogCluster{
	{name: "billing-db", host: "billing-db", app: "BILLING", team: "DBA", env: "PRD", site: sitePA1, hw: hwDB, role: "db", nodes: 2, services: []catalogService{
		{name: "billing-pgsql", ns: "prd", topology: "failover", kinds: kindsDB, image: "postgres:16.4"},
		{name: "billing-batch", ns: "prd", topology: "failover", kinds: kindsApp, image: "registry.example.com/billing/batch:4.12.1"},
	}},
	{name: "billing-app", host: "billing-app", app: "BILLING", team: "WebOps", env: "PRD", site: sitePA1, hw: hwVM, role: "app", nodes: 3, services: []catalogService{
		{name: "billing-api", ns: "prd", topology: "flex", kinds: kindsApp, image: "registry.example.com/billing/api:4.12.1"},
		{name: "billing-portal", ns: "prd", topology: "flex", kinds: kindsWeb, image: "registry.example.com/billing/portal:2.8.0"},
	}},
	{name: "billing-uat", host: "billing-uat", app: "BILLING", team: "WebOps", env: "TST", site: siteLY2, hw: hwVM, role: "app", nodes: 2, services: []catalogService{
		{name: "billing-pgsql", ns: "uat", topology: "failover", kinds: kindsDB, image: "postgres:16.4"},
		{name: "billing-api", ns: "uat", topology: "flex", kinds: kindsApp, image: "registry.example.com/billing/api:4.13.0-rc2"},
	}},
	{name: "crm-db", host: "crm-db", app: "CRM", team: "DBA", env: "PRD", site: sitePA1, hw: hwDB, role: "db", nodes: 2, services: []catalogService{
		{name: "crm-mariadb", ns: "prd", topology: "failover", kinds: kindsDB, image: "mariadb:11.4"},
	}},
	{name: "crm-app", host: "crm-app", app: "CRM", team: "WebOps", env: "PRD", site: sitePA1, hw: hwVM, role: "app", nodes: 2, services: []catalogService{
		{name: "crm-web", ns: "prd", topology: "flex", kinds: kindsApp, image: "registry.example.com/crm/web:7.3.2"},
		{name: "crm-worker", ns: "prd", topology: "failover", kinds: kindsApp, image: "registry.example.com/crm/worker:7.3.2"},
	}},
	{name: "erp-p01", host: "sap-p01-", app: "ERP", team: "SAPBasis", env: "PRD", site: sitePA1, hw: hwSAP, role: "sap", nodes: 2, services: []catalogService{
		{name: "sap-p01-hana", ns: "prd", topology: "failover", kinds: kindsSAP},
		{name: "sap-p01-ascs", ns: "prd", topology: "failover", kinds: kindsInfra},
		{name: "sap-p01-pas", ns: "prd", topology: "failover", kinds: kindsInfra},
	}},
	{name: "erp-q01", host: "sap-q01-", app: "ERP", team: "SAPBasis", env: "PPRD", site: siteLY2, hw: hwSAP, role: "sap", nodes: 2, services: []catalogService{
		{name: "sap-q01-hana", ns: "pre", topology: "failover", kinds: kindsSAP},
		{name: "sap-q01-ascs", ns: "pre", topology: "failover", kinds: kindsInfra},
	}},
	{name: "web-front", host: "web", app: "WEB", team: "WebOps", env: "PRD", site: sitePA1, hw: hwVMSmall, role: "web", nodes: 4, services: []catalogService{
		{name: "www-nginx", ns: "prd", topology: "flex", kinds: kindsWeb, image: "nginx:1.27.2"},
		{name: "edge-haproxy", ns: "prd", topology: "flex", kinds: kindsWeb, image: "haproxy:3.0.5"},
	}},
	{name: "payment", host: "pay", app: "PAYMENT", team: "WebOps", env: "PRD", site: sitePA1, hw: hwVM, role: "pay", nodes: 2, services: []catalogService{
		{name: "payment-gw", ns: "prd", topology: "failover", kinds: kindsApp, image: "registry.example.com/payment/gateway:3.1.4"},
		{name: "payment-vault", ns: "prd", topology: "failover", kinds: kindsDB, image: "hashicorp/vault:1.17.6"},
	}},
	{name: "data-kafka", host: "kafka", app: "DATA", team: "DataOps", env: "PRD", site: siteLY2, hw: hwData, role: "kfk", nodes: 3, services: []catalogService{
		{name: "kafka", ns: "prd", topology: "flex", kinds: kindsData, image: "apache/kafka:3.8.0"},
		{name: "schema-registry", ns: "prd", topology: "failover", kinds: kindsSimple, image: "confluentinc/cp-schema-registry:7.7.1"},
	}},
	{name: "data-search", host: "es", app: "DATA", team: "DataOps", env: "PRD", site: siteLY2, hw: hwData, role: "es", nodes: 3, services: []catalogService{
		{name: "elasticsearch", ns: "prd", topology: "flex", kinds: kindsData, image: "elasticsearch:8.15.2"},
		{name: "kibana", ns: "prd", topology: "failover", kinds: kindsWeb, image: "kibana:8.15.2"},
	}},
	{name: "infra-core", host: "infra", app: "INFRA", team: "UnixOps", env: "PRD", site: sitePA1, hw: hwVMSmall, role: "inf", nodes: 2, services: []catalogService{
		{name: "dns", ns: "ops", topology: "flex", kinds: kindsInfra},
		{name: "ldap", ns: "ops", topology: "failover", kinds: kindsDB, image: "osixia/openldap:1.5.0"},
		{name: "ntp", ns: "ops", topology: "flex", kinds: kindsInfra},
	}},
	{name: "infra-mon", host: "mon", app: "INFRA", team: "UnixOps", env: "PRD", site: siteLY2, hw: hwVM, role: "mon", nodes: 2, services: []catalogService{
		{name: "prometheus", ns: "ops", topology: "failover", kinds: kindsData, image: "prom/prometheus:v2.54.1"},
		{name: "grafana", ns: "ops", topology: "failover", kinds: kindsWeb, image: "grafana/grafana:11.2.2"},
		{name: "alertmanager", ns: "ops", topology: "failover", kinds: kindsSimple, image: "prom/alertmanager:v0.27.0"},
	}},
	{name: "dev", host: "dev", app: "BILLING", team: "WebOps", env: "DEV", site: siteLY2, hw: hwVMSmall, role: "dev", nodes: 2, services: []catalogService{
		{name: "billing-api", ns: "dev", topology: "failover", kinds: kindsApp, image: "registry.example.com/billing/api:4.14.0-dev"},
		{name: "crm-web", ns: "dev", topology: "failover", kinds: kindsApp, image: "registry.example.com/crm/web:7.4.0-dev"},
		{name: "ci-runner", ns: "dev", topology: "flex", kinds: kindsSimple, image: "gitlab/gitlab-runner:v17.4.0"},
	}},
}

// appTeams are the ops teams of the apps of the catalog, in order.
var appTeams = [][2]string{
	{"BILLING", "WebOps"}, {"CRM", "WebOps"}, {"ERP", "SAPBasis"}, {"WEB", "WebOps"},
	{"PAYMENT", "WebOps"}, {"DATA", "DataOps"}, {"INFRA", "UnixOps"},
}

// catalogApps are the apps of the catalog and their ops team, in order.
func catalogApps() [][2]string { return appTeams }

// newCatalogFleet builds the fleet of the realistic catalog.
func newCatalogFleet(spec fleetSpec) (*fleet, error) {
	f := &fleet{spec: spec}
	si := 0
	for ci, cc := range realisticCatalog {
		c := &cluster{
			idx:  ci,
			id:   spec.uuid("cluster", cc.name),
			name: strings.ToLower(cc.site.code) + "-" + cc.name,
			app:  cc.app,
		}
		for ni := 0; ni < cc.nodes; ni++ {
			name := fmt.Sprintf("%s-%s%02d", strings.ToLower(cc.site.code), cc.host, ni+1)
			c.nodes = append(c.nodes, &node{
				idx:     ni,
				name:    name,
				id:      spec.uuid("node", name),
				uuid:    spec.uuid("auth", name),
				cluster: c,
				profile: &nodeProfile{env: cc.env, team: cc.team, role: cc.role, site: cc.site, hw: cc.hw,
					rack: fmt.Sprintf("R%02d", 10+(ci*3+ni)%12)},
			})
		}
		for _, cs := range cc.services {
			c.services = append(c.services, newCatalogService(spec, c, si, cs))
			si++
		}
		f.clusters = append(f.clusters, c)
	}
	return f, nil
}

func newCatalogService(spec fleetSpec, c *cluster, si int, cs catalogService) *service {
	r := spec.newRand("service", cs.ns, cs.name)
	s := &service{
		idx:      si,
		name:     cs.name,
		path:     cs.ns + "/svc/" + cs.name,
		topology: cs.topology,
		app:      c.app,
		primary:  r.IntN(len(c.nodes)),
	}
	if len(c.nodes) < 2 {
		s.topology = "failover"
	}
	counts := map[string]int{}
	for _, kind := range cs.kinds {
		s.resources = append(s.resources, newCatalogResource(r, s, kind, counts[kind], cs.image))
		counts[kind]++
	}
	return s
}

// fsMounts are the mount points of the file systems of a service, in order.
var fsMounts = []string{"data", "log", "backup"}

func newCatalogResource(r *rand.Rand, s *service, kind string, i int, image string) resource {
	short := strings.TrimPrefix(strings.TrimPrefix(s.name, "sap-"), "billing-")
	switch kind {
	case "ip":
		ip := fmt.Sprintf("10.%d.%d.%d", 30+s.idx/250%50, 10+s.idx%200, 10+i)
		return resource{rid: fmt.Sprintf("ip#%d", i), kind: "ip.host", label: ip + "/24 bond0",
			info: map[string]any{"dev": "bond0", "ipaddr": ip, "netmask": 24, "hostname": s.name}}
	case "vg":
		vg := "vg_" + strings.ReplaceAll(s.name, "-", "_")
		return resource{rid: fmt.Sprintf("disk#%d", i), kind: "disk.vg", label: vg, info: map[string]any{"name": vg}}
	case "fs":
		vg := "vg_" + strings.ReplaceAll(s.name, "-", "_")
		mnt := "/srv/" + s.name + "/" + fsMounts[i%len(fsMounts)]
		dev := "/dev/" + vg + "/lv_" + fsMounts[i%len(fsMounts)]
		return resource{rid: fmt.Sprintf("fs#%d", i), kind: "fs.xfs", label: dev + " " + mnt,
			info: map[string]any{"mnt": mnt, "dev": dev}}
	case "container":
		if image == "" {
			image = "registry.example.com/" + s.name + ":1.0"
		}
		name := fmt.Sprintf("%s..%s.container.%d", namespaceOf(s.path), s.name, i)
		return resource{rid: fmt.Sprintf("container#%d", i), kind: "container.podman", label: "podman " + image,
			info: map[string]any{"name": name, "id": fmt.Sprintf("%016x%016x", r.Uint64(), r.Uint64()), "pid": 1000 + r.IntN(100000)}}
	case "app":
		script := "/opt/" + s.name + "/bin/" + short + "ctl"
		return resource{rid: fmt.Sprintf("app#%d", i), kind: "app.forking", label: script,
			info: map[string]any{"script": script}}
	default:
		return resource{rid: fmt.Sprintf("sync#%d", i), kind: "sync.rsync", label: "rsync /srv/" + s.name + "/backup to peers",
			optional: true, info: map[string]any{"target": []string{"nodes"}}}
	}
}

// realisticPackages are the packages of a node of the catalog: the base system
// and what its role adds.
func realisticPackages(n *node) []string {
	base := []string{"bash", "coreutils", "glibc", "openssl", "openssh-server", "openssh-clients", "systemd", "sudo",
		"curl", "wget", "rsync", "tar", "gzip", "chrony", "rsyslog", "logrotate", "lvm2", "xfsprogs", "e2fsprogs",
		"iproute", "iptables-nft", "nftables", "tcpdump", "lsof", "strace", "vim-enhanced", "less", "procps-ng",
		"python3", "python3-libs", "perl", "jq", "git", "audit", "selinux-policy", "policycoreutils", "dnf",
		"kernel", "kernel-core", "linux-firmware", "NetworkManager", "dbus", "polkit", "cronie", "pam", "shadow-utils",
		"ca-certificates", "tzdata", "libxml2", "zlib", "sqlite", "nss", "krb5-libs", "sssd", "realmd", "opensvc-server",
		"node_exporter", "zabbix-agent2", "filebeat"}
	if n.profile != nil && n.profile.hw.pkgType == "deb" {
		base = []string{"bash", "coreutils", "libc6", "openssl", "openssh-server", "openssh-client", "systemd", "sudo",
			"curl", "wget", "rsync", "tar", "gzip", "chrony", "rsyslog", "logrotate", "lvm2", "xfsprogs", "e2fsprogs",
			"iproute2", "nftables", "tcpdump", "lsof", "strace", "vim", "less", "procps", "python3", "python3-minimal",
			"perl", "jq", "git", "auditd", "apparmor", "apt", "linux-image-generic", "linux-firmware", "netplan.io",
			"dbus", "polkitd", "cron", "libpam-modules", "passwd", "ca-certificates", "tzdata", "libxml2", "zlib1g",
			"sqlite3", "libnss3", "libkrb5-3", "sssd", "realmd", "opensvc-server", "prometheus-node-exporter",
			"zabbix-agent2", "filebeat"}
	}
	role := map[string][]string{
		"db":  {"postgresql16", "postgresql16-server", "postgresql16-contrib", "pgbackrest", "mariadb-server", "mariadb-backup"},
		"app": {"podman", "buildah", "skopeo", "containers-common", "java-21-openjdk-headless"},
		"web": {"podman", "containers-common", "certbot"},
		"sap": {"sapconf", "saptune", "uuidd", "libatomic1", "insserv-compat"},
		"kfk": {"podman", "openjdk-21-jre-headless", "kcat"},
		"es":  {"podman", "openjdk-21-jre-headless"},
		"inf": {"bind", "bind-utils", "openldap-servers", "chrony"},
		"mon": {"podman", "prometheus", "alertmanager", "grafana"},
		"pay": {"podman", "containers-common", "vault"},
		"dev": {"podman", "gitlab-runner", "make", "gcc"},
	}
	if n.profile == nil {
		return base
	}
	return append(base, role[n.profile.role]...)
}

// appDescriptions describe the apps of the catalog.
var appDescriptions = map[string]string{
	"BILLING": "Customer billing and invoicing",
	"CRM":     "Customer relationship management",
	"ERP":     "SAP S/4HANA finance and supply chain",
	"WEB":     "Public web front and edge proxies",
	"PAYMENT": "Card payment gateway (PCI DSS scope)",
	"DATA":    "Event streaming and search platform",
	"INFRA":   "Core infrastructure: DNS, LDAP, NTP, monitoring",
}
