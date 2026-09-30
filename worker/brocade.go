package worker

import (
	"fmt"
	"strconv"
	"strings"
)

type (
	// brocadeSwitch is what the switchshow, nsshow and zoneshow outputs of a
	// brocade switch say, as the old collector read them in
	// init/modules/brocade.py: the switch, its ports, the aliases of every
	// zoning configuration, and the zones of the effective one.
	brocadeSwitch struct {
		Name  string
		Model string
		WWN   string

		// Ports are keyed by port index.
		Ports map[string]*brocadePort

		// Alias is the ports of each alias, by configuration name.
		Alias map[string]map[string][]string

		// Cfg is the name of the effective zoning configuration, empty when
		// the switch reports none.
		Cfg string

		// Zone is the ports of each zone of the effective configuration.
		Zone map[string][]string
	}

	brocadePort struct {
		Index          string
		Slot           string
		Port           string
		Type           string
		State          string
		RemotePortName string

		// Speed is in Gb/s, 0 when negotiated but not yet known. Nego says
		// the speed is negotiated, and is empty when the output does not
		// tell.
		Speed int
		Nego  string

		// NSE are the port names the name server reports behind the port,
		// as the devices logged in through an NPIV port.
		NSE []string

		trunkMaster *[2]string
	}
)

// parseBrocade reads the outputs of the switchshow, nsshow and zoneshow
// commands of a brocade switch.
func parseBrocade(switchshow, nsshow, zoneshow string) (*brocadeSwitch, error) {
	s := &brocadeSwitch{
		Ports: make(map[string]*brocadePort),
		Alias: make(map[string]map[string][]string),
		Zone:  make(map[string][]string),
	}
	if err := s.loadSwitchshow(switchshow); err != nil {
		return nil, err
	}
	s.loadNsshow(nsshow)
	s.loadZoneshow(zoneshow)
	return s, nil
}

// loadSwitchshow reads the switch identity and its port table:
//
//	switchName:	sansw1
//	switchType:	71.2
//	switchWwn:	10:00:00:05:33:a0:e6:42
//	...
//	Index Port Address Media Speed State     Proto
//	==================================================
//	  0   0   010000   id    N8   Online      FC  F-Port  50:01:43:80:07:2d:f5:e6
//	  1   1   010100   id    N8   Online      FC  E-Port  10:00:00:05:33:a0:e6:43 "sansw2" (downstream)
//
// The words past the columns of the table are a comment saying the port
// type and the name of the port at the other end.
func (s *brocadeSwitch) loadSwitchshow(buff string) error {
	lines := strings.Split(buff, "\n")
	if len(lines) < 2 {
		return fmt.Errorf("switchshow is empty")
	}
	var (
		start, commentIdx int
		cols              []string
	)
	for i, line := range lines {
		switch {
		case strings.HasPrefix(line, "switchName:"):
			s.Name = strings.ToLower(strings.TrimSpace(afterColon(line)))
		case strings.HasPrefix(line, "switchType"):
			s.Model = strings.TrimSpace(afterColon(line))
		case strings.HasPrefix(line, "switchWwn"):
			_, v, _ := strings.Cut(line, ":")
			s.WWN = strings.TrimSpace(strings.ReplaceAll(v, ":", ""))
		case strings.HasPrefix(line, "===="):
			start = i + 1
			commentIdx = len(line)
			if i > 0 {
				cols = strings.Fields(lines[i-1])
			}
		}
		if start > 0 {
			break
		}
	}
	// A label with no port table says nothing of the ports: parsed as a
	// switch with no port, it would have the job delete all its rows.
	if start == 0 || len(cols) == 0 {
		return fmt.Errorf("switchshow has no port table")
	}
	rindex := make(map[[2]string]string)
	for _, line := range lines[start:] {
		// A line shorter than the rule under the header is passed over, as
		// the old collector did: an unlit port has no comment and is one,
		// and the tables have never held those.
		if len(line) < commentIdx {
			continue
		}
		var comment string
		if words := strings.Fields(line); len(words) >= len(cols)+1 {
			comment = strings.Join(words[len(cols):], " ")
		}
		values := make(map[string]string)
		for i, v := range strings.Fields(line[:commentIdx]) {
			if i >= len(cols) {
				break
			}
			values[cols[i]] = v
		}
		port := &brocadePort{
			Index: values["Index"],
			Slot:  "0",
			Port:  values["Port"],
			Type:  values["Type"],
			State: values["State"],
		}
		if v, ok := values["Slot"]; ok {
			port.Slot = v
		}
		if v, ok := values["Area"]; ok {
			port.Index = v
		}
		if comment != "" {
			switch {
			case strings.Contains(comment, "E-Port"):
				port.Type = "E-Port"
			case strings.Contains(comment, "F-Port"):
				port.Type = "F-Port"
			}
			words := strings.Fields(comment)
			if len(words) >= 2 && strings.Contains(words[1], ":") {
				port.RemotePortName = portName(words[1])
			} else if len(words) >= 3 && strings.Contains(words[2], ":") {
				port.RemotePortName = portName(words[2])
			}
			if strings.Contains(comment, "master is Port") {
				// (Trunk port, master is Port  3 )
				parts := strings.Split(comment, "Port")
				master := strings.TrimSpace(strings.Trim(parts[len(parts)-1], ") "))
				port.trunkMaster = &[2]string{port.Slot, master}
			} else if strings.Contains(comment, "master is Slot") {
				// (Trunk port, master is Slot  1 Port  0 )
				slot, sport := wordAfter(words, "Slot"), wordAfter(words, "Port")
				port.trunkMaster = &[2]string{slot, sport}
			}
		}
		speed := values["Speed"]
		switch {
		case speed == "AN":
			port.Speed, port.Nego = 0, "F"
		case strings.HasPrefix(speed, "N"):
			port.Speed, _ = strconv.Atoi(strings.TrimPrefix(speed, "N"))
			port.Nego = "T"
		case strings.HasSuffix(speed, "G"):
			port.Speed, _ = strconv.Atoi(strings.TrimSuffix(speed, "G"))
			port.Nego = "F"
		}
		rindex[[2]string{port.Slot, port.Port}] = port.Index
		s.Ports[port.Index] = port
	}
	// A slave port of a trunk takes the remote port name of its master.
	for _, port := range s.Ports {
		if port.trunkMaster == nil {
			continue
		}
		if i, ok := rindex[*port.trunkMaster]; ok {
			if master, ok := s.Ports[i]; ok {
				port.RemotePortName = master.RemotePortName
			}
		}
	}
	return nil
}

// loadNsshow adds to each port the port names the name server reports
// behind it:
//
//	Type Pid    COS     PortName                NodeName                 TTL(sec)
//	N    020f01;      3;50:01:43:80:07:2d:f5:e6;50:01:43:80:07:2d:f5:e7; na
//	    FC4s: FCP
//	    Port Index: 32
func (s *brocadeSwitch) loadNsshow(buff string) {
	var name string
	for _, line := range strings.Split(buff, "\n") {
		if len(line) <= 2 {
			continue
		}
		if line[1] != ' ' {
			// a new entry
			if fields := strings.Split(line, ";"); len(fields) == 5 {
				name = strings.ReplaceAll(fields[2], ":", "")
			}
			continue
		}
		if strings.HasPrefix(strings.TrimSpace(line), "Port Index:") {
			index := strings.TrimSpace(afterLastColon(line))
			if port, ok := s.Ports[index]; ok && name != "" {
				port.NSE = append(port.NSE, name)
			}
		}
	}
}

// loadZoneshow reads the aliases of each zoning configuration, from the
// defined configurations, and the zones of the effective configuration:
//
//	Defined configuration:
//	 cfg:   PROD_CFG
//	 alias: DMX0197_8A0
//	                50:06:04:84:52:A4:F9:47
//	 alias: Wdms01  10:00:00:00:C9:24:32:8C
//	 alias: EVA04   50:00:1F:E1:50:21:90:19; 50:00:1F:E1:50:21:90:1F;
//	                50:00:1F:E1:50:21:90:1B; 50:00:1F:E1:50:21:90:1D
//
//	Effective configuration:
//	 cfg:   PROD_CFG
//	 zone:  Wzzs01_DMX1370_9B1
//	                50:06:04:84:52:a6:1e:b8
//	                10:00:00:00:c9:3a:12:72
func (s *brocadeSwitch) loadZoneshow(buff string) {
	lines := strings.Split(buff, "\n")
	effective := 0
	cfg := ""
	haveCfg := false
firstPass:
	for i, raw := range lines {
		line := strings.TrimSpace(raw)
		switch {
		case strings.HasPrefix(line, "alias:"):
			if !haveCfg {
				continue
			}
			words := strings.Fields(line)
			var alias string
			var ports []string
			switch len(words) {
			case 2:
				alias = words[1]
				if i+1 < len(lines) {
					ports = []string{portName(strings.TrimSpace(lines[i+1]))}
				}
			case 3:
				alias = words[1]
				ports = []string{portName(words[2])}
			case 4:
				alias = words[1]
				ports = []string{portName(words[2]), portName(words[3])}
				for j := i + 1; j < len(lines); j++ {
					next := lines[j]
					if strings.Contains(next, "alias") || len(next) == 0 {
						break
					}
					for _, w := range strings.Fields(next) {
						ports = append(ports, portName(w))
					}
				}
			default:
				continue
			}
			s.Alias[cfg][alias] = ports
		case strings.HasPrefix(line, "Effective configuration:"):
			effective = i
			break firstPass
		case strings.HasPrefix(line, "cfg:"):
			cfg = strings.TrimSpace(afterLastColon(line))
			haveCfg = true
			s.Alias[cfg] = make(map[string][]string)
		}
	}

	lines = lines[effective:]
	for i, raw := range lines {
		line := strings.TrimSpace(raw)
		switch {
		case strings.HasPrefix(line, "zone:"):
			words := strings.Fields(line)
			zone := words[len(words)-1]
			members := make([]string, 0)
			for _, next := range lines[i+1:] {
				next = strings.TrimSpace(next)
				if strings.HasPrefix(next, "zone:") || len(next) == 0 {
					break
				}
				members = append(members, strings.ToLower(strings.ReplaceAll(next, ":", "")))
			}
			s.Zone[zone] = members
		case strings.HasPrefix(line, "cfg:"):
			s.Cfg = strings.TrimSpace(afterLastColon(line))
		}
	}
}

// portName is a port wwn as the tables store it: lower case, without its
// colons and its list separator.
func portName(s string) string {
	s = strings.ReplaceAll(s, ":", "")
	s = strings.ReplaceAll(s, ";", "")
	return strings.ToLower(s)
}

// afterColon returns what follows the first colon of s.
func afterColon(s string) string {
	_, v, _ := strings.Cut(s, ":")
	return v
}

// afterLastColon returns what follows the last colon of s.
func afterLastColon(s string) string {
	if i := strings.LastIndex(s, ":"); i >= 0 {
		return s[i+1:]
	}
	return s
}

// wordAfter returns the word following word in words, without the closing
// parenthesis a comment may end it with.
func wordAfter(words []string, word string) string {
	for i, w := range words {
		if w == word && i+1 < len(words) {
			return strings.TrimSpace(strings.Trim(words[i+1], ")"))
		}
	}
	return ""
}

// rows are the lines of the switches table the switch fills, one per device
// the name server reports behind a port, or one for the port when it
// reports none: the column sw_portname holds the switch wwn, as the old
// collector filled it.
func (s *brocadeSwitch) rows() []any {
	l := make([]any, 0, len(s.Ports))
	for _, p := range s.Ports {
		row := func(rportname string) map[string]any {
			return map[string]any{
				"sw_name":      s.Name,
				"sw_portname":  s.WWN,
				"sw_index":     atoiOrNil(p.Index),
				"sw_slot":      atoiOrNil(p.Slot),
				"sw_port":      atoiOrNil(p.Port),
				"sw_portspeed": p.Speed,
				"sw_portnego":  p.Nego,
				"sw_portstate": p.State,
				"sw_porttype":  p.Type,
				"sw_rportname": rportname,
			}
		}
		n := 0
		for _, nse := range p.NSE {
			if nse == p.RemotePortName {
				continue
			}
			l = append(l, row(nse))
			n++
		}
		if n == 0 {
			l = append(l, row(p.RemotePortName))
		}
	}
	return l
}

// atoiOrNil returns the integer s writes, or nil for a column the output
// did not fill.
func atoiOrNil(s string) any {
	if i, err := strconv.Atoi(s); err == nil {
		return i
	}
	return nil
}
