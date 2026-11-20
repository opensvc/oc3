package cdb

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// The startup graph of a service instance, as the historical collector's
// service "startup" tab drew it (json_startup_data): the resources of the
// service on one node, in the order the agent starts them, with their state.
//
// The order is om3's (core/object ResourceSets, core_action passes):
//   - the driver groups, in the order ip, volume, disk, fs, share, container,
//     app (v2 configurations add hb and stonith before them, and name the
//     disk drivers vg, zpool, drbd...);
//   - within a group, the resources outside any subset first, then the
//     subsets by name, each run in parallel or in series (subset#<group>:<name>
//     or subset#<name>, parallel keyword);
//   - within a set, the resources by rid, an app whose start keyword is a
//     sequence number by that number;
//   - an ip whose netns (v2: container_rid) names a container starts right
//     after it rather than in its group;
//   - the triggers of a resource around it, those of the service around all;
//   - the encap resources last, inside the containers of the encap nodes.
//
// The sync and task resources are not started, nor drawn.

// StartupVertex is a step of the startup graph.
type StartupVertex struct {
	ID string `json:"id"`
	// Kind is service, node, subset, resource, trigger or encap.
	Kind  string `json:"kind"`
	Label string `json:"label"`
	// RID of a resource, of the resource of a trigger, of the container of an
	// encap node.
	RID string `json:"rid,omitempty"`
	// Family is the driver group of a resource: ip, disk, fs, container...
	Family string `json:"family,omitempty"`
	// Type is the driver of a resource: docker, netns, flag...
	Type string `json:"type,omitempty"`
	// Desc is the description the agent reports for a resource; the command
	// of a trigger.
	Desc string `json:"desc,omitempty"`
	// Status of a resource, as the agent last reported it.
	Status string `json:"status,omitempty"`
	// Trigger is the hook of a trigger: blocking_pre_start, pre_start,
	// blocking_post_start, post_start.
	Trigger  string `json:"trigger,omitempty"`
	Parallel bool   `json:"parallel,omitempty"`
	Disabled bool   `json:"disabled,omitempty"`
	Optional bool   `json:"optional,omitempty"`
	Monitor  bool   `json:"monitor,omitempty"`
	Standby  bool   `json:"standby,omitempty"`
	Encap    bool   `json:"encap,omitempty"`
	// Node is the encap node a resource runs in.
	Node string `json:"node,omitempty"`
}

// StartupEdge is an edge of the startup graph: "sequence", from a step to a
// step that follows it, or "requires", from a resource to one whose start
// waits for it to reach the states of the label (start_requires).
type StartupEdge struct {
	From  string `json:"from"`
	To    string `json:"to"`
	Kind  string `json:"kind"`
	Label string `json:"label,omitempty"`
}

// StartupNode is a node of the service, to draw the startup on.
type StartupNode struct {
	NodeID   string `json:"node_id"`
	Nodename string `json:"nodename"`
}

// Startup is the startup graph of a service on a node.
type Startup struct {
	SvcID    string          `json:"svc_id"`
	Svcname  string          `json:"svcname"`
	NodeID   string          `json:"node_id,omitempty"`
	Nodename string          `json:"nodename,omitempty"`
	Nodes    []StartupNode   `json:"nodes"`
	Vertices []StartupVertex `json:"vertices"`
	Edges    []StartupEdge   `json:"edges"`
}

// StartupResourceState is what the agent last reported of a resource (resmon).
type StartupResourceState struct {
	Status   string
	Type     string
	Desc     string
	Disabled bool
	Optional bool
	Monitor  bool
}

// svcConfig is a service configuration: its sections in order, and their
// keywords.
type svcConfig struct {
	sections []string
	values   map[string]map[string]string
}

// parseSvcConfig reads a service configuration as the agents write it: ini
// sections, "key = value" lines, values continued on indented lines or held
// between backquotes, comments starting with # or ;. The v2 agents sent it
// with its new lines escaped.
func parseSvcConfig(text string) *svcConfig {
	if !strings.Contains(text, "\n") && strings.Contains(text, `\n`) {
		text = strings.ReplaceAll(text, `\n`, "\n")
	}
	c := &svcConfig{values: map[string]map[string]string{}}
	section := ""
	key := ""
	inQuote := false
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimRight(line, "\r")
		if inQuote {
			c.values[section][key] += "\n" + line
			if strings.Contains(line, "`") {
				inQuote = false
				c.values[section][key] = strings.Trim(c.values[section][key], "`")
			}
			continue
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, ";") {
			continue
		}
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			section = strings.TrimSpace(trimmed[1 : len(trimmed)-1])
			key = ""
			if _, ok := c.values[section]; !ok {
				c.values[section] = map[string]string{}
				c.sections = append(c.sections, section)
			}
			continue
		}
		if section == "" {
			continue
		}
		if (line[0] == ' ' || line[0] == '\t') && key != "" {
			c.values[section][key] += "\n" + trimmed
			continue
		}
		k, v, ok := strings.Cut(trimmed, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if strings.HasPrefix(v, "`") {
			if strings.Count(v, "`") >= 2 {
				v = strings.Trim(v, "`")
			} else {
				inQuote = true
			}
		}
		c.values[section][key] = v
	}
	return c
}

// raw returns the value of a keyword of a section as written, unscoped.
func (c *svcConfig) raw(section, key string) (string, bool) {
	v, ok := c.values[section][key]
	return v, ok
}

// get returns the value of a keyword of a section on a node: that scoped to
// the node, else to its role (nodes, drpnodes, encapnodes), else unscoped.
func (c *svcConfig) get(section, key, nodename, role string) (string, bool) {
	for _, k := range []string{key + "@" + nodename, key + "@" + role, key} {
		if v, ok := c.values[section][k]; ok {
			return v, true
		}
	}
	return "", false
}

func (c *svcConfig) getBool(section, key, nodename, role string) bool {
	v, ok := c.get(section, key, nodename, role)
	if !ok {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "true", "yes", "on", "1", "t", "y":
		return true
	}
	return false
}

func (c *svcConfig) list(section, key, nodename, role string) []string {
	v, _ := c.get(section, key, nodename, role)
	return strings.Fields(v)
}

// startupGroups are the driver groups the agents start, in their order: om3's
// ip to app, after the v2 heartbeats and stonith.
var startupGroups = []string{"hb", "stonith", "ip", "volume", "disk", "fs", "share", "container", "app"}

// startupGroupOf returns the driver group of a resource section, "" for a
// section the agents do not start.
func startupGroupOf(section string) string {
	family, _, ok := strings.Cut(section, "#")
	if !ok {
		return ""
	}
	switch family {
	case "vg", "drbd", "pool", "zpool", "loop", "raw", "rados", "lock", "gandi", "md", "scsireserv":
		return "disk"
	}
	for _, g := range startupGroups {
		if g == family {
			return g
		}
	}
	return ""
}

type startupResource struct {
	rid      string
	group    string
	subset   string
	sortKey  string
	linkTo   string
	encap    bool
	disabled bool
	vertex   StartupVertex
}

type startupBuilder struct {
	c        *svcConfig
	nodename string
	role     string
	s        *Startup
	vertices map[string]bool
	edges    map[string]bool
}

func (b *startupBuilder) addVertex(v StartupVertex) {
	if b.vertices[v.ID] {
		return
	}
	b.vertices[v.ID] = true
	b.s.Vertices = append(b.s.Vertices, v)
}

func (b *startupBuilder) addEdge(from, to, kind, label string) {
	if from == to {
		return
	}
	k := from + "\x00" + to + "\x00" + kind
	if b.edges[k] {
		return
	}
	b.edges[k] = true
	b.s.Edges = append(b.s.Edges, StartupEdge{From: from, To: to, Kind: kind, Label: label})
}

// follow adds the sequence edges from the steps before to a step, and returns
// the step as the one the next follows.
func (b *startupBuilder) follow(tails []string, id string) []string {
	for _, t := range tails {
		b.addEdge(t, id, "sequence", "")
	}
	return []string{id}
}

var startTriggersPre = []string{"blocking_pre_start", "pre_start"}
var startTriggersPost = []string{"blocking_post_start", "post_start"}

// triggers adds the triggers of hooks of a section in sequence, prefix telling
// their ids apart.
func (b *startupBuilder) triggers(tails []string, section, prefix string, hooks []string) []string {
	for _, hook := range hooks {
		cmd, ok := b.c.get(section, hook, b.nodename, b.role)
		if !ok || strings.TrimSpace(cmd) == "" {
			continue
		}
		id := "trigger:" + prefix + ":" + hook
		rid := ""
		if section != "DEFAULT" {
			rid = section
		}
		b.addVertex(StartupVertex{ID: id, Kind: "trigger", Label: hook, RID: rid, Trigger: hook, Desc: cmd})
		tails = b.follow(tails, id)
	}
	return tails
}

// resourceStep adds a resource with its triggers and the resources linked to
// it, after the steps before, and returns the steps the next follows.
func (b *startupBuilder) resourceStep(tails []string, r *startupResource, prefix string, linked map[string][]*startupResource) []string {
	tails = b.triggers(tails, r.rid, prefix+r.rid, startTriggersPre)
	b.addVertex(r.vertex)
	tails = b.follow(tails, r.vertex.ID)
	tails = b.triggers(tails, r.rid, prefix+r.rid, startTriggersPost)
	for _, l := range linked[r.rid] {
		tails = b.resourceStep(tails, l, prefix, linked)
	}
	return tails
}

// pass adds the resources of a pass of the agent in their order: by group,
// then by set, in series or in parallel, after the steps before. It returns
// the steps the next follows.
func (b *startupBuilder) pass(tails []string, resources []*startupResource, prefix string) []string {
	linked := map[string][]*startupResource{}
	has := map[string]bool{}
	for _, r := range resources {
		has[r.rid] = true
	}
	var main []*startupResource
	for _, r := range resources {
		if r.linkTo != "" && has[r.linkTo] {
			linked[r.linkTo] = append(linked[r.linkTo], r)
			continue
		}
		main = append(main, r)
	}
	for _, l := range linked {
		sortStartupResources(l)
	}
	for _, g := range startupGroups {
		sets := map[string][]*startupResource{}
		for _, r := range main {
			if r.group == g {
				sets[r.subset] = append(sets[r.subset], r)
			}
		}
		names := make([]string, 0, len(sets))
		for name := range sets {
			names = append(names, name)
		}
		// The resources outside any subset ("") first.
		sort.Strings(names)
		for _, name := range names {
			set := sets[name]
			sortStartupResources(set)
			parallel := false
			if name != "" {
				section := "subset#" + g + ":" + name
				if _, ok := b.c.values[section]; !ok {
					if _, ok := b.c.values["subset#"+name]; ok {
						section = "subset#" + name
					}
				}
				parallel = b.c.getBool(section, "parallel", b.nodename, b.role)
				id := "subset:" + prefix + g + ":" + name
				b.addVertex(StartupVertex{ID: id, Kind: "subset", Label: "subset#" + g + ":" + name, Parallel: parallel})
				tails = b.follow(tails, id)
			}
			if parallel {
				var next []string
				for _, r := range set {
					next = append(next, b.resourceStep(tails, r, prefix, linked)...)
				}
				tails = next
				continue
			}
			for _, r := range set {
				tails = b.resourceStep(tails, r, prefix, linked)
			}
		}
	}
	return tails
}

func sortStartupResources(l []*startupResource) {
	sort.SliceStable(l, func(i, j int) bool { return l[i].sortKey < l[j].sortKey })
}

// isStartSequence says an app start keyword is a sequence number, as om3's
// resapp isSequenceNumber.
func isStartSequence(s string) bool {
	if len(s) < 2 {
		return false
	}
	_, err := strconv.ParseInt(s, 10, 16)
	return err == nil
}

// nodeRole returns the role of a node in a service configuration: the scope
// of its scoped keywords.
func (c *svcConfig) nodeRole(nodename string) string {
	short := strings.Split(nodename, ".")[0]
	in := func(key string) bool {
		for _, n := range c.list("DEFAULT", key, nodename, "") {
			if n == nodename || strings.Split(n, ".")[0] == short {
				return true
			}
		}
		return false
	}
	switch {
	case in("encapnodes"):
		return "encapnodes"
	case in("drpnodes"), in("drpnode"):
		return "drpnodes"
	}
	return "nodes"
}

// isDisabled says a resource is disabled on a node: disable, or the v2
// disable_on naming the role of the node.
func (c *svcConfig) isDisabled(section, nodename, role string) bool {
	if c.getBool(section, "disable", nodename, role) {
		return true
	}
	for _, r := range c.list(section, "disable_on", nodename, role) {
		if r == role {
			return true
		}
	}
	return false
}

// BuildStartup builds the startup graph of a service configuration on a node.
//
// states are the resources the agent of the node last reported, by rid;
// encapStates those of the encap nodes, by encap node name then rid. Without
// showDisabled, the disabled resources, which the agent skips, are left out.
func BuildStartup(config, svcname, nodename string, states map[string]StartupResourceState, encapStates map[string]map[string]StartupResourceState, showDisabled bool) *Startup {
	c := parseSvcConfig(config)
	role := c.nodeRole(nodename)
	b := &startupBuilder{
		c:        c,
		nodename: nodename,
		role:     role,
		s:        &Startup{Nodes: []StartupNode{}, Vertices: []StartupVertex{}, Edges: []StartupEdge{}},
		vertices: map[string]bool{},
		edges:    map[string]bool{},
	}
	b.addVertex(StartupVertex{ID: "service", Kind: "service", Label: svcname})
	tails := []string{"service"}
	if nodename != "" {
		b.addVertex(StartupVertex{ID: "node", Kind: "node", Label: nodename})
		tails = b.follow(tails, "node")
	}
	tails = b.triggers(tails, "DEFAULT", "service", startTriggersPre)

	resource := func(section, encapNode string, st map[string]StartupResourceState) *startupResource {
		group := startupGroupOf(section)
		if group == "" {
			return nil
		}
		r := &startupResource{rid: section, group: group}
		r.subset, _ = c.get(section, "subset", nodename, role)
		tags := c.list(section, "tags", nodename, role)
		hasTag := func(tag string) bool {
			for _, t := range tags {
				if t == tag {
					return true
				}
			}
			return false
		}
		r.encap = c.getBool(section, "encap", nodename, role) || hasTag("encap")
		// v2: the ip, disks and fs of a zone are started by the zone.
		if hasTag("zone") && (group == "ip" || group == "disk" || group == "fs") {
			return nil
		}
		state, reported := st[section]
		typ, _ := c.get(section, "type", nodename, role)
		if typ == "" && state.Type != "" {
			_, typ, _ = strings.Cut(state.Type, ".")
		}
		r.sortKey = section
		if group == "app" {
			if start, _ := c.get(section, "start", nodename, role); isStartSequence(start) {
				r.sortKey = start + " " + section
			}
		}
		if group == "ip" {
			if netns, _ := c.get(section, "netns", nodename, role); strings.HasPrefix(netns, "container#") {
				r.linkTo = netns
			} else if rid, _ := c.get(section, "container_rid", nodename, role); rid != "" {
				r.linkTo = rid
			}
		}
		r.disabled = c.isDisabled(section, nodename, role) || state.Disabled
		id := "resource:" + section
		if encapNode != "" {
			id = "resource:" + encapNode + ":" + section
		}
		r.vertex = StartupVertex{
			ID:       id,
			Kind:     "resource",
			Label:    section,
			RID:      section,
			Family:   group,
			Type:     typ,
			Desc:     state.Desc,
			Status:   state.Status,
			Disabled: r.disabled,
			Optional: c.getBool(section, "optional", nodename, role) || state.Optional,
			Monitor:  c.getBool(section, "monitor", nodename, role) || state.Monitor,
			Standby:  c.getBool(section, "standby", nodename, role) || hasTag("noaction"),
			Encap:    r.encap,
			Node:     encapNode,
		}
		if !reported {
			r.vertex.Status = ""
		}
		return r
	}

	var host []*startupResource
	var encap []string
	for _, section := range c.sections {
		r := resource(section, "", states)
		if r == nil || (r.disabled && !showDisabled) {
			continue
		}
		if r.encap {
			encap = append(encap, section)
			continue
		}
		host = append(host, r)
	}
	tails = b.pass(tails, host, "")

	// The encap pass: in each container of an encap node, in the order of
	// the containers, the encap resources.
	if len(encap) > 0 {
		for _, encapNode := range c.list("DEFAULT", "encapnodes", nodename, role) {
			container := encapContainer(c, encapNode, nodename, role)
			id := "encap:" + encapNode
			b.addVertex(StartupVertex{ID: id, Kind: "encap", Label: encapNode, RID: container, Node: encapNode})
			if container != "" && b.vertices["resource:"+container] {
				b.addEdge("resource:"+container, id, "requires", "")
			}
			tails = b.follow(tails, id)
			encapRole := "encapnodes"
			var l []*startupResource
			for _, section := range encap {
				r := resource(section, encapNode, encapStates[encapNode])
				if r == nil {
					continue
				}
				r.disabled = r.disabled || c.isDisabled(section, encapNode, encapRole)
				r.vertex.Disabled = r.disabled
				if r.disabled && !showDisabled {
					continue
				}
				l = append(l, r)
			}
			tails = b.pass(tails, l, encapNode+":")
		}
	}

	b.triggers(tails, "DEFAULT", "service", startTriggersPost)

	// The requirements of the resources drawn: the steps a start waits for.
	for _, v := range b.s.Vertices {
		if v.Kind != "resource" {
			continue
		}
		prefix := "resource:"
		if v.Node != "" {
			prefix += v.Node + ":"
		}
		for _, req := range parseStartRequires(c.list(v.RID, "start_requires", nodename, role)) {
			from := prefix + req.rid
			if !b.vertices[from] {
				continue
			}
			b.addEdge(from, v.ID, "requires", req.states)
		}
	}
	return b.s
}

// encapContainer returns the container of an encap node: the one named after
// it, else the only container of the configuration.
func encapContainer(c *svcConfig, encapNode, nodename, role string) string {
	short := strings.Split(encapNode, ".")[0]
	var containers []string
	for _, section := range c.sections {
		if !strings.HasPrefix(section, "container#") {
			continue
		}
		containers = append(containers, section)
		if name, _ := c.get(section, "name", nodename, role); strings.Split(name, ".")[0] == short {
			return section
		}
	}
	if len(containers) == 1 {
		return containers[0]
	}
	return ""
}

type startRequirement struct {
	rid    string
	states string
}

// parseStartRequires reads the words of a start_requires: "ip#0 fs#0(down,stdby down)".
// The states of a requirement may hold spaces: its words are joined back
// until the parenthesis closes.
func parseStartRequires(words []string) []startRequirement {
	var l []startRequirement
	for i := 0; i < len(words); i++ {
		w := words[i]
		if strings.Contains(w, "(") {
			for !strings.Contains(w, ")") && i+1 < len(words) {
				i++
				w += " " + words[i]
			}
		}
		rid, states, ok := strings.Cut(w, "(")
		if !ok {
			l = append(l, startRequirement{rid: w})
			continue
		}
		l = append(l, startRequirement{rid: rid, states: strings.TrimSuffix(states, ")")})
	}
	return l
}

// String helps the tests read a graph.
func (e StartupEdge) String() string {
	if e.Label != "" {
		return fmt.Sprintf("%s -%s(%s)-> %s", e.From, e.Kind, e.Label, e.To)
	}
	return fmt.Sprintf("%s -%s-> %s", e.From, e.Kind, e.To)
}
