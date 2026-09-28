package worker

import (
	"encoding/json"
	"testing"
)

func TestResourcesStatusGroups(t *testing.T) {
	cases := []struct {
		name      string
		resources string
		expected  map[string]string
	}{
		{
			name:      "nil resources",
			resources: `null`,
			expected:  map[string]string{},
		},
		{
			name:      "empty resources",
			resources: `{}`,
			expected:  map[string]string{},
		},
		{
			name:      "single up",
			resources: `{"fs#1": {"type": "fs.flag", "status": "up"}}`,
			expected:  map[string]string{"fs": "up"},
		},
		{
			name: "up and down is warn",
			resources: `{
				"fs#1": {"type": "fs.flag", "status": "up"},
				"fs#2": {"type": "fs.host", "status": "down"}}`,
			expected: map[string]string{"fs": "warn"},
		},
		{
			name: "up and stdby up is up",
			resources: `{
				"disk#1": {"type": "disk.vg", "status": "up"},
				"disk#2": {"type": "disk.vg", "status": "stdby up", "standby": true}}`,
			expected: map[string]string{"disk": "up"},
		},
		{
			name: "down and stdby up is stdby up",
			resources: `{
				"disk#1": {"type": "disk.vg", "status": "down"},
				"disk#2": {"type": "disk.vg", "status": "stdby up", "standby": true}}`,
			expected: map[string]string{"disk": "stdby up"},
		},
		{
			name: "only stdby up is stdby up",
			resources: `{
				"ip#1": {"type": "ip.host", "status": "stdby up", "standby": true}}`,
			expected: map[string]string{"ip": "stdby up"},
		},
		{
			name: "n/a and up is up",
			resources: `{
				"app#1": {"type": "app.forking", "status": "n/a"},
				"app#2": {"type": "app.simple", "status": "up"}}`,
			expected: map[string]string{"app": "up"},
		},
		{
			name: "disabled resource is ignored",
			resources: `{
				"app#1": {"type": "app.forking", "status": "up"},
				"app#2": {"type": "app.forking", "status": "down", "disable": true}}`,
			expected: map[string]string{"app": "up"},
		},
		{
			name: "encap resource is counted",
			resources: `{
				"app#1": {"type": "app.forking", "status": "down", "encap": true}}`,
			expected: map[string]string{"app": "down"},
		},
		{
			name: "optional resource is counted",
			resources: `{
				"app#1": {"type": "app.forking", "status": "up"},
				"app#2": {"type": "app.forking", "status": "down", "optional": true}}`,
			expected: map[string]string{"app": "warn"},
		},
		{
			name: "nostatus tag is n/a",
			resources: `{
				"app#1": {"type": "app.forking", "status": "down", "tags": ["foo", "nostatus"]}}`,
			expected: map[string]string{"app": "n/a"},
		},
		{
			name: "undef and unknown status are ignored",
			resources: `{
				"app#1": {"type": "app.forking", "status": "undef"},
				"app#2": {"type": "app.forking", "status": "foo"},
				"app#3": {"type": "app.forking"}}`,
			expected: map[string]string{"app": "n/a"},
		},
		{
			name:      "sync up is n/a",
			resources: `{"sync#1": {"type": "sync.rsync", "status": "up"}}`,
			expected:  map[string]string{"sync": "n/a"},
		},
		{
			name:      "sync down is warn",
			resources: `{"sync#1": {"type": "sync.rsync", "status": "down"}}`,
			expected:  map[string]string{"sync": "warn"},
		},
		{
			name:      "sync stdby down is kept",
			resources: `{"sync#1": {"type": "sync.rsync", "status": "stdby down"}}`,
			expected:  map[string]string{"sync": "stdby down"},
		},
		{
			name: "unknown type is ignored",
			resources: `{
				"foo#1": {"type": "foo.bar", "status": "down"},
				"bad": "not a map"}`,
			expected: map[string]string{},
		},
		{
			name: "type without driver",
			resources: `{
				"volume#1": {"type": "volume", "status": "up"},
				"task#1": {"type": "task.host", "status": "down"}}`,
			expected: map[string]string{"volume": "up", "task": "down"},
		},
		{
			name: "several groups",
			resources: `{
				"ip#1": {"type": "ip.host", "status": "up"},
				"disk#1": {"type": "disk.vg", "status": "down"},
				"fs#1": {"type": "fs.flag", "status": "warn"},
				"share#1": {"type": "share.nfs", "status": "stdby down"},
				"container#1": {"type": "container.podman", "status": "up"},
				"app#1": {"type": "app.forking", "status": "stdby up"},
				"sync#1": {"type": "sync.rsync", "status": "up"}}`,
			expected: map[string]string{
				"ip":        "up",
				"disk":      "down",
				"fs":        "warn",
				"share":     "stdby down",
				"container": "up",
				"app":       "stdby up",
				"sync":      "n/a",
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var resources map[string]any
			if err := json.Unmarshal([]byte(c.resources), &resources); err != nil {
				t.Fatal(err)
			}
			got := resourcesStatusGroups(resources)
			if len(got) != len(statusGroups) {
				t.Errorf("expected %d groups, got %d: %v", len(statusGroups), len(got), got)
			}
			for _, group := range statusGroups {
				expected, ok := c.expected[group]
				if !ok {
					expected = "n/a"
				}
				if got[group] != expected {
					t.Errorf("group %s: expected %q, got %q", group, expected, got[group])
				}
			}
		})
	}
}

func TestContainerStatusGroupV3(t *testing.T) {
	var encap map[string]any
	err := json.Unmarshal([]byte(`{
		"container#1": {
			"hostname": "vm1",
			"avail": "up",
			"overall": "up",
			"resources": {
				"app#1": {"type": "app.forking", "status": "up", "encap": true},
				"fs#1": {"type": "fs.flag", "status": "down", "encap": true}
			}
		}
	}`), &encap)
	if err != nil {
		t.Fatal(err)
	}
	var resources map[string]any
	if err := json.Unmarshal([]byte(`{
		"container#1": {"type": "container.podman", "status": "up"}}`), &resources); err != nil {
		t.Fatal(err)
	}
	hypervisor := &instanceData{encap: encap, resources: resources}
	for group, s := range resourcesStatusGroups(resources) {
		switch group {
		case "ip":
			hypervisor.MonIpStatus = s
		case "disk":
			hypervisor.MonDiskStatus = s
		case "fs":
			hypervisor.MonFsStatus = s
		case "share":
			hypervisor.MonShareStatus = s
		case "container":
			hypervisor.MonContainerStatus = s
		case "app":
			hypervisor.MonAppStatus = s
		case "sync":
			hypervisor.MonSyncStatus = s
		}
	}
	hypervisor.MonAvailStatus = "up"
	hypervisor.MonOverallStatus = "up"

	c := hypervisor.Container("container#1")
	expected := map[string]string{
		"ip":        "n/a",
		"disk":      "n/a",
		"fs":        "down",
		"share":     "n/a",
		"container": "up",
		"app":       "up",
		"sync":      "n/a",
	}
	got := map[string]string{
		"ip":        c.MonIpStatus,
		"disk":      c.MonDiskStatus,
		"fs":        c.MonFsStatus,
		"share":     c.MonShareStatus,
		"container": c.MonContainerStatus,
		"app":       c.MonAppStatus,
		"sync":      c.MonSyncStatus,
	}
	for group, s := range expected {
		if got[group] != s {
			t.Errorf("group %s: expected %q, got %q", group, s, got[group])
		}
	}
	if c.MonVmName != "vm1" {
		t.Errorf("expected vm name vm1, got %q", c.MonVmName)
	}
	if c.MonVmType != "podman" {
		t.Errorf("expected vm type podman, got %q", c.MonVmType)
	}
}
