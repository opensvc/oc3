// Package safe stores the files of the collector safe: the legacy plaintext
// files, laid out as the historical collector's web2py upload field does, and
// the files encrypted at rest with a per-file key wrapped by a master key.
package safe

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// KeySize is the size of the master keys and of the per-file keys: AES-256.
const KeySize = 32

// Key is a master key of the safe, named by an id stored along each per-file
// key it wraps, so that the files keep opening after a rotation.
type Key struct {
	ID     string
	Secret []byte
}

// Keyring holds the master keys: the active one wraps the keys of the files
// stored from now on; the others still unwrap the keys of the files stored
// before a rotation.
type Keyring struct {
	active *Key
	byID   map[string]*Key
}

// keysFile is the layout of the keys file:
//
//	keys:
//	  - id: "2026-10"
//	    key: <base64 of 32 random bytes>
//	    active: true
type keysFile struct {
	Keys []struct {
		ID     string `yaml:"id"`
		Key    string `yaml:"key"`
		Active bool   `yaml:"active"`
	} `yaml:"keys"`
}

// ErrNoKey tells a safe without master key: its files are stored in plaintext.
var ErrNoKey = errors.New("safe: no master key")

// LoadKeyring reads the keys file. The file holds secrets: it must not be
// readable by its group or by others, and one key exactly must be active.
func LoadKeyring(path string) (*Keyring, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("safe keys file: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("safe keys file %s: mode %04o lets its group or others read it, use 0600", path, info.Mode().Perm())
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("safe keys file: %w", err)
	}
	return ParseKeyring(b)
}

// ParseKeyring reads the content of a keys file. The messages never quote a key.
func ParseKeyring(b []byte) (*Keyring, error) {
	var f keysFile
	if err := yaml.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("safe keys file: invalid yaml")
	}
	ring := &Keyring{byID: map[string]*Key{}}
	for i, entry := range f.Keys {
		if entry.ID == "" {
			return nil, fmt.Errorf("safe keys file: key #%d has no id", i)
		}
		if _, dup := ring.byID[entry.ID]; dup {
			return nil, fmt.Errorf("safe keys file: key id %q is listed twice", entry.ID)
		}
		secret, err := base64.StdEncoding.DecodeString(entry.Key)
		if err != nil || len(secret) != KeySize {
			return nil, fmt.Errorf("safe keys file: key %q must be the base64 of %d bytes", entry.ID, KeySize)
		}
		key := &Key{ID: entry.ID, Secret: secret}
		ring.byID[entry.ID] = key
		if entry.Active {
			if ring.active != nil {
				return nil, fmt.Errorf("safe keys file: keys %q and %q are both active", ring.active.ID, entry.ID)
			}
			ring.active = key
		}
	}
	if len(ring.byID) == 0 {
		return nil, ErrNoKey
	}
	if ring.active == nil {
		return nil, fmt.Errorf("safe keys file: no key is active")
	}
	return ring, nil
}

// Active returns the key wrapping the keys of the files stored from now on.
func (r *Keyring) Active() *Key {
	if r == nil {
		return nil
	}
	return r.active
}

// Get returns the key of an id, nil when the keyring no longer holds it.
func (r *Keyring) Get(id string) *Key {
	if r == nil {
		return nil
	}
	return r.byID[id]
}
