package serverhandlers

import (
	"sort"
	"strconv"
	"strings"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// sanTopology builds the SAN wiring graph of a node from its adapters, the target
// ports zoned to them, and the ports of the switches.
//
// Each adapter is looked up among the remote ports of the switches: the switches
// found are the entry switches, always shown, even when no zoned target is
// reached from them — an adapter plugged in a fabric that leads nowhere is worth
// seeing. From a switch, a port whose remote port is a target zoned to the adapter
// is a link to the array owning it; an inter-switch link (E-Port) is followed to
// the next switch, which is kept, with the link, only if it leads to such a
// target. A switch already on the path is not entered again.
//
// This is the graph the historical collector handed to graphviz (sandata, in
// ajax_node.py), with two differences: entry switches without a path to a target
// were dropped there, and the ranks were graphviz ranks rather than columns.
func sanTopology(nodeID, nodename string, endpoints []cdb.SANEndpoint, ports []cdb.SwitchPort) server.SanTopology {
	b := sanBuilder{
		bySwitch: map[string][]cdb.SwitchPort{},
		byRemote: map[string][]cdb.SwitchPort{},
		depth:    map[string]int{},
		links:    map[string]server.SanTopologyLink{},
		arrays:   map[string]bool{},
	}
	for _, p := range ports {
		b.bySwitch[p.WWN] = append(b.bySwitch[p.WWN], p)
		b.byRemote[p.Remote] = append(b.byRemote[p.Remote], p)
	}

	// The targets zoned to each adapter, with their array; adapters in order.
	targets := map[string]map[string]string{}
	var hbas []string
	for _, e := range endpoints {
		if _, ok := targets[e.HBA]; !ok {
			targets[e.HBA] = map[string]string{}
			hbas = append(hbas, e.HBA)
		}
		if e.Target != "" {
			targets[e.HBA][e.Target] = e.Array
		}
	}

	serverID := "server:" + nodeID
	for _, hba := range hbas {
		for _, p := range b.byRemote[hba] {
			b.link("hba:"+hba+":"+p.WWN+":"+strconv.Itoa(p.Index), server.SanTopologyLink{
				Tail: serverID, TailPort: hba,
				Head: switchID(p.WWN), HeadPort: strconv.Itoa(p.Index),
				Speeds: []int{p.Speed},
			})
			b.keep(p.WWN, 0)
			b.walk(p.WWN, 0, map[string]bool{p.WWN: true}, targets[hba])
		}
	}
	return b.graph(serverID, nodename)
}

type sanBuilder struct {
	bySwitch map[string][]cdb.SwitchPort
	byRemote map[string][]cdb.SwitchPort
	// depth is the number of inter-switch links between the node and each switch
	// kept, the longest way when there are several.
	depth  map[string]int
	links  map[string]server.SanTopologyLink
	arrays map[string]bool
}

func switchID(wwn string) string { return "switch:" + wwn }
func arrayID(name string) string { return "array:" + name }

func (b *sanBuilder) link(key string, l server.SanTopologyLink) {
	if _, ok := b.links[key]; !ok {
		b.links[key] = l
	}
}

func (b *sanBuilder) keep(wwn string, depth int) {
	if d, ok := b.depth[wwn]; !ok || depth > d {
		b.depth[wwn] = depth
	}
}

// walk follows the ports of a switch and reports whether a zoned target is
// reached from it, directly or through other switches.
func (b *sanBuilder) walk(wwn string, depth int, path map[string]bool, targets map[string]string) bool {
	reached := false
	for _, p := range b.bySwitch[wwn] {
		if array, ok := targets[p.Remote]; ok {
			b.arrays[array] = true
			b.link("tgt:"+wwn+":"+strconv.Itoa(p.Index)+":"+p.Remote, server.SanTopologyLink{
				Tail: switchID(wwn), TailPort: strconv.Itoa(p.Index),
				Head: arrayID(array), HeadPort: p.Remote,
				Speeds: []int{p.Speed},
			})
			reached = true
			continue
		}
		if p.Type != "E-Port" || path[p.Remote] {
			continue
		}
		if _, isSwitch := b.bySwitch[p.Remote]; !isSwitch {
			continue
		}
		path[p.Remote] = true
		leads := b.walk(p.Remote, depth+1, path, targets)
		delete(path, p.Remote)
		if !leads {
			continue
		}
		b.keep(p.Remote, depth+1)
		b.trunk(wwn, p.Remote)
		reached = true
	}
	return reached
}

// trunk adds the link between two switches: one link whatever the number of
// inter-switch links between them, its ports being the indexes of the members on
// each side.
func (b *sanBuilder) trunk(tail, head string) {
	key := "isl:" + tail + ":" + head
	if tail > head {
		key = "isl:" + head + ":" + tail
	}
	if _, ok := b.links[key]; ok {
		return
	}
	var tailPorts, headPorts []string
	var speeds []int
	for _, p := range b.bySwitch[tail] {
		if p.Remote == head {
			tailPorts = append(tailPorts, strconv.Itoa(p.Index))
			speeds = append(speeds, p.Speed)
		}
	}
	for _, p := range b.bySwitch[head] {
		if p.Remote == tail {
			headPorts = append(headPorts, strconv.Itoa(p.Index))
		}
	}
	b.links[key] = server.SanTopologyLink{
		Tail: switchID(tail), TailPort: strings.Join(tailPorts, ","),
		Head: switchID(head), HeadPort: strings.Join(headPorts, ","),
		Speeds: speeds,
	}
}

func (b *sanBuilder) graph(serverID, nodename string) server.SanTopology {
	links := make([]server.SanTopologyLink, 0, len(b.links))
	ports := map[string]map[string]bool{}
	use := func(id, port string) {
		if ports[id] == nil {
			ports[id] = map[string]bool{}
		}
		ports[id][port] = true
	}
	for _, l := range b.links {
		links = append(links, l)
		use(l.Tail, l.TailPort)
		use(l.Head, l.HeadPort)
	}
	sort.Slice(links, func(i, j int) bool {
		a, c := links[i], links[j]
		if a.Tail != c.Tail {
			return a.Tail < c.Tail
		}
		if a.TailPort != c.TailPort {
			return portLess(a.TailPort, c.TailPort)
		}
		return a.Head+a.HeadPort < c.Head+c.HeadPort
	})
	portsOf := func(id string) []string {
		out := make([]string, 0, len(ports[id]))
		for p := range ports[id] {
			out = append(out, p)
		}
		sort.Slice(out, func(i, j int) bool { return portLess(out[i], out[j]) })
		return out
	}

	nodes := []server.SanTopologyNode{{
		Id: serverID, Kind: server.Server, Label: nodename, Rank: 0, Ports: portsOf(serverID),
	}}
	last := 0
	for wwn, depth := range b.depth {
		first := b.bySwitch[wwn][0]
		fabric := first.Fabric
		nodes = append(nodes, server.SanTopologyNode{
			Id: switchID(wwn), Kind: server.Switch, Label: first.Name,
			Fabric: &fabric, Rank: depth + 1, Ports: portsOf(switchID(wwn)),
		})
		if depth+1 > last {
			last = depth + 1
		}
	}
	for name := range b.arrays {
		nodes = append(nodes, server.SanTopologyNode{
			Id: arrayID(name), Kind: server.Array, Label: name,
			Rank: last + 1, Ports: portsOf(arrayID(name)),
		})
	}
	sort.Slice(nodes, func(i, j int) bool {
		a, c := nodes[i], nodes[j]
		if a.Rank != c.Rank {
			return a.Rank < c.Rank
		}
		if a.Label != c.Label {
			return a.Label < c.Label
		}
		return a.Id < c.Id
	})
	return server.SanTopology{Nodes: nodes, Links: links}
}

// portLess orders port labels: switch port indexes by number ("2" before "10", a
// trunk by its first member), the others as text.
func portLess(a, b string) bool {
	na, errA := strconv.Atoi(strings.SplitN(a, ",", 2)[0])
	nb, errB := strconv.Atoi(strings.SplitN(b, ",", 2)[0])
	if errA == nil && errB == nil && na != nb {
		return na < nb
	}
	return a < b
}
