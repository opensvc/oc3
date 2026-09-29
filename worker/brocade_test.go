package worker

import (
	"reflect"
	"sort"
	"testing"
)

// The outputs of a Fabric OS switch, as the agent pushes them, in the formats
// the old collector parser was written against.
const (
	testSwitchshow = `switchName:	SANSW1
switchType:	71.2
switchState:	Online
switchMode:	Native
switchRole:	Principal
switchDomain:	1
switchId:	fffc01
switchWwn:	10:00:00:05:33:a0:e6:42
zoning:		ON (PROD_CFG)
switchBeacon:	OFF

Index Port Address Media Speed State     Proto
==================================================
  0   0   010000   id    N8   Online      FC  F-Port  50:01:43:80:07:2d:f5:e6
  1   1   010100   id    N16  Online      FC  F-Port  1 N Port + 2 NPIV public
  2   2   010200   id    8G   Online      FC  E-Port  10:00:00:05:33:a0:e6:43 "sansw2" (downstream)
  3   3   010300   id    8G   Online      FC  E-Port  (Trunk port, master is Port  2 )
  4   4   010400   id    AN   No_Light    FC
`
	testNsshow = `{
 Type Pid    COS     PortName                NodeName                 TTL(sec)
 N    010000;      3;50:01:43:80:07:2d:f5:e6;50:01:43:80:07:2d:f5:e7; na
    FC4s: FCP
    Port Index: 0
 N    010100;      3;20:00:00:25:b5:00:00:01;20:00:00:25:b5:00:00:ff; na
    FC4s: FCP
    Port Index: 1
 N    010101;      3;20:00:00:25:b5:00:00:02;20:00:00:25:b5:00:00:ff; na
    FC4s: FCP
    Port Index: 1
 N    019900;      3;20:00:00:25:b5:00:00:09;20:00:00:25:b5:00:00:ff; na
    Port Index: 99
The Local Name Server has 4 entries }
`
	testZoneshow = `Defined configuration:
 cfg:	PROD_CFG
 alias:	DMX0197_8A0
		50:06:04:84:52:A4:F9:47
 alias:	Wdms01	10:00:00:00:C9:24:32:8C
 alias:	EVA04	50:00:1F:E1:50:21:90:19; 50:00:1F:E1:50:21:90:1F;
		50:00:1F:E1:50:21:90:1B; 50:00:1F:E1:50:21:90:1D

Effective configuration:
 cfg:	PROD_CFG
 zone:	Wzzs01_DMX1370_9B1
		50:06:04:84:52:a6:1e:b8
		10:00:00:00:c9:3a:12:72
 zone:	Wdms01_EVA04
		10:00:00:00:c9:24:32:8c
		50:00:1f:e1:50:21:90:19

`
)

func equal(t *testing.T, what string, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s: got %#v, want %#v", what, got, want)
	}
}

func parseTestBrocade(t *testing.T) *brocadeSwitch {
	t.Helper()
	s, err := parseBrocade(testSwitchshow, testNsshow, testZoneshow)
	if err != nil {
		t.Fatalf("parse: %s", err)
	}
	return s
}

func TestParseBrocadeSwitch(t *testing.T) {
	s := parseTestBrocade(t)
	equal(t, "name", s.Name, "sansw1")
	equal(t, "model", s.Model, "71.2")
	equal(t, "wwn", s.WWN, "1000000533a0e642")
	equal(t, "port count, the unlit port 4 passed over as the old collector did", len(s.Ports), 4)

	p := s.Ports["0"]
	equal(t, "port 0 type", p.Type, "F-Port")
	equal(t, "port 0 state", p.State, "Online")
	equal(t, "port 0 remote", p.RemotePortName, "5001438007"+"2df5e6")
	equal(t, "port 0 speed", p.Speed, 8)
	equal(t, "port 0 nego", p.Nego, "T")

	p = s.Ports["1"]
	equal(t, "port 1 speed", p.Speed, 16)
	equal(t, "port 1 remote, no wwn in the comment of an npiv port", p.RemotePortName, "")
	equal(t, "port 1 name server entries", p.NSE, []string{"20000025b5000001", "20000025b5000002"})

	p = s.Ports["2"]
	equal(t, "port 2 type", p.Type, "E-Port")
	equal(t, "port 2 speed", p.Speed, 8)
	equal(t, "port 2 nego", p.Nego, "F")
	equal(t, "port 2 remote", p.RemotePortName, "1000000533a0e643")

	p = s.Ports["3"]
	equal(t, "port 3 type", p.Type, "E-Port")
	equal(t, "port 3 remote, taken from its trunk master", p.RemotePortName, "1000000533a0e643")
}

func TestParseBrocadeZoning(t *testing.T) {
	s := parseTestBrocade(t)
	equal(t, "effective cfg", s.Cfg, "PROD_CFG")
	equal(t, "aliases", s.Alias["PROD_CFG"], map[string][]string{
		"DMX0197_8A0": {"5006048452a4f947"},
		"Wdms01":      {"10000000c924328c"},
		"EVA04":       {"50001fe150219019", "50001fe15021901f", "50001fe15021901b", "50001fe15021901d"},
	})
	equal(t, "zones", s.Zone, map[string][]string{
		"Wzzs01_DMX1370_9B1": {"5006048452a61eb8", "10000000c93a1272"},
		"Wdms01_EVA04":       {"10000000c924328c", "50001fe150219019"},
	})
}

// A port behind which the name server reports devices gets a row per
// device, and a port with none gets one row, with the port at the other end.
func TestBrocadeRows(t *testing.T) {
	s := parseTestBrocade(t)
	var got []string
	for _, r := range s.rows() {
		m := r.(map[string]any)
		equal(t, "sw_name", m["sw_name"], "sansw1")
		equal(t, "sw_portname", m["sw_portname"], "1000000533a0e642")
		got = append(got, m["sw_rportname"].(string))
	}
	sort.Strings(got)
	equal(t, "remote port names", got, []string{
		"1000000533a0e643",
		"1000000533a0e643",
		"20000025b5000001",
		"20000025b5000002",
		"50014380072df5e6",
	})
}

// A switchshow without its port table is refused rather than read as a
// switch without ports, which would have the job delete them all.
func TestParseBrocadeRefusesATruncatedSwitchshow(t *testing.T) {
	if _, err := parseBrocade("switchName:	sansw1\nswitchWwn:	10:00:00:05:33:a0:e6:42\n", "", ""); err == nil {
		t.Error("a switchshow without a port table is accepted")
	}
	if _, err := parseBrocade("", "", ""); err == nil {
		t.Error("an empty switchshow is accepted")
	}
}
