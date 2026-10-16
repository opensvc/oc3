package safe

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
)

// nameColumnSize is the size of safe.uuid, which holds the stored name.
const nameColumnSize = 512

// regexpName is a stored name, as the web2py upload field of the historical
// collector makes them: "safe.uuid.<uuid key>.<original name in base16>.<ext>",
// the encoded name missing when the original one is empty. The om3 agent accepts
// the same names in its safe:// references.
var regexpName = regexp.MustCompile(`^safe\.uuid\.([0-9a-fA-F-]{2,})(\.[0-9a-fA-F]*)?\.[A-Za-z0-9_]+$`)

// regexpExtension is the extension web2py keeps from the original name.
var regexpExtension = regexp.MustCompile(`\.(\w{1,5})$`)

// ErrName tells a stored name that is not one: refused before any path is built.
var ErrName = errors.New("safe: invalid stored file name")

// Store is the directory of the safe files, shared with the historical collector
// while both run: its upload folder for the safe table.
type Store struct {
	Dir string
	// FileMode is the mode of the files written; the directories get the
	// execute bits of the classes the files can be read by.
	FileMode fs.FileMode
}

// NewName returns the stored name of a new file, as web2py's Field.store makes
// it, so that the historical collector reads the files oc3 writes.
func NewName(original string) (string, error) {
	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	key := hex.EncodeToString(raw)
	ext := "txt"
	if m := regexpExtension.FindStringSubmatch(original); m != nil {
		ext = m[1]
	}
	name := "safe.uuid." + key + "." + hex.EncodeToString([]byte(original))
	if max := nameColumnSize - 1 - len(ext); len(name) > max {
		name = name[:max]
	}
	return name + "." + ext, nil
}

// OriginalName returns the name the file was uploaded with, decoded from its
// stored name; empty when it does not hold one.
func OriginalName(name string) string {
	m := regexpName.FindStringSubmatch(name)
	if m == nil || len(m[2]) < 2 {
		return ""
	}
	b, err := hex.DecodeString(m[2][1:])
	if err != nil {
		return ""
	}
	return string(b)
}

// Path returns where a stored name lives: "<dir>/safe.uuid/<two first characters
// of its uuid key>/<name>", web2py's layout with uploadseparate.
func (s Store) Path(name string) (string, error) {
	m := regexpName.FindStringSubmatch(name)
	if m == nil || filepath.Base(name) != name {
		return "", ErrName
	}
	return filepath.Join(s.Dir, "safe.uuid", m[1][:2], name), nil
}

// Read returns the content stored under a name, as it is on disk.
func (s Store) Read(name string) ([]byte, error) {
	path, err := s.Path(name)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(path)
}

// Write stores a content under a name, atomically: written to a temporary file of
// the same directory, synced, then renamed over the name. A crash leaves the
// previous content or the new one, never a part of it.
func (s Store) Write(name string, content []byte) error {
	path, err := s.Path(name)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	mode := s.FileMode
	if mode == 0 {
		mode = 0o640
	}
	if err := os.MkdirAll(dir, dirMode(mode)); err != nil {
		return fmt.Errorf("safe: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("safe: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(content); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("safe: %w", err)
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("safe: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("safe: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("safe: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("safe: %w", err)
	}
	// The rename itself is durable once the directory is synced.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// Remove deletes the file stored under a name; one already gone is no error.
func (s Store) Remove(name string) error {
	path, err := s.Path(name)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("safe: %w", err)
	}
	return nil
}

// dirMode gives a directory the execute bit of each class the files are readable by.
func dirMode(file fs.FileMode) fs.FileMode {
	mode := file | 0o700
	for _, bits := range [][2]fs.FileMode{{0o040, 0o010}, {0o004, 0o001}} {
		if file&bits[0] != 0 {
			mode |= bits[1]
		}
	}
	return mode
}
