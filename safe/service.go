package safe

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/opensvc/oc3/cdb"
)

// Mode says whether the files stored from now on are encrypted at rest.
type Mode string

const (
	// ModeOff stores in plaintext, a master key or not: while the historical
	// collector, which reads only plaintext, shares the directory.
	ModeOff Mode = "off"
	// ModeOptional encrypts when a master key is configured, stores in
	// plaintext otherwise.
	ModeOptional Mode = "optional"
	// ModeRequired refuses to start without a master key.
	ModeRequired Mode = "required"
)

// ParseMode reads the safe.encryption setting.
func ParseMode(s string) (Mode, error) {
	switch m := Mode(s); m {
	case ModeOff, ModeOptional, ModeRequired:
		return m, nil
	case "":
		return ModeOptional, nil
	}
	return "", fmt.Errorf("safe.encryption: %q is none of off, optional, required", s)
}

// ErrCompromised tells a file whose content no longer has the digest recorded at
// upload, as lib_safe_download() refuses it.
var ErrCompromised = errors.New("safe: the file is compromised: its content does not match the recorded md5")

// ErrSealedWithoutKey tells an encrypted file the database does not describe as
// such: it cannot be opened, and is not served as if it were plaintext.
var ErrSealedWithoutKey = errors.New("safe: the file is encrypted but its row records no key")

// Service reads and writes the safe files: the store on disk, the master keys,
// and the rows describing them.
type Service struct {
	Store   Store
	Ring    *Keyring
	Mode    Mode
	MaxSize int64
}

// Encrypts tells whether the files stored from now on are encrypted.
func (s *Service) Encrypts() bool {
	return s.Mode != ModeOff && s.Ring.Active() != nil
}

// Check validates the setting at start: a required encryption needs a key.
func (s *Service) Check() error {
	if s.Mode == ModeRequired && s.Ring.Active() == nil {
		return fmt.Errorf("safe.encryption is required, but no master key is configured (safe.keys_file)")
	}
	return nil
}

func digests(b []byte) (string, string) {
	m := md5.Sum(b)
	h := sha256.Sum256(b)
	return hex.EncodeToString(m[:]), hex.EncodeToString(h[:])
}

// Put writes a new file under a new stored name, made from the name of the
// uploaded file, encrypted when the setting asks for it, and returns what its row
// records; the caller sets the name to tell it by. The digests are those of the
// plaintext: the agents compare the md5 with their copy.
func (s *Service) Put(original string, plaintext []byte) (cdb.SafeFileContent, error) {
	if s.MaxSize > 0 && int64(len(plaintext)) > s.MaxSize {
		return cdb.SafeFileContent{}, fmt.Errorf("safe: the file is %d bytes, more than the %d bytes allowed", len(plaintext), s.MaxSize)
	}
	name, err := NewName(original)
	if err != nil {
		return cdb.SafeFileContent{}, err
	}
	sumMD5, sumSHA256 := digests(plaintext)
	c := cdb.SafeFileContent{Name: original, UUID: name, Size: int64(len(plaintext)), MD5: sumMD5, SHA256: sumSHA256}
	content := plaintext
	if s.Encrypts() {
		sealed, err := Seal(s.Ring, name, plaintext)
		if err != nil {
			return cdb.SafeFileContent{}, err
		}
		v := Version
		c.EncVersion, c.EncKeyID, c.WrappedKey = &v, sealed.KeyID, sealed.WrappedKey
		content = sealed.Content
	}
	if err := s.Store.Write(name, content); err != nil {
		return cdb.SafeFileContent{}, err
	}
	return c, nil
}

// Get returns the plaintext of a row's file, decrypted when it is encrypted, and
// checked against the md5 recorded at upload.
func (s *Service) Get(f *cdb.SafeFile) ([]byte, error) {
	content, err := s.Store.Read(f.UUID)
	if err != nil {
		return nil, err
	}
	plaintext := content
	switch {
	case f.Encrypted():
		if plaintext, err = Open(s.Ring, f.UUID, content, f.EncKeyID, f.WrappedKey); err != nil {
			return nil, err
		}
	case IsSealed(content):
		return nil, ErrSealedWithoutKey
	}
	if f.MD5 != "" {
		if sum, _ := digests(plaintext); sum != f.MD5 {
			return nil, ErrCompromised
		}
	}
	return plaintext, nil
}

// Encrypt converts a row's plaintext file in place: the same stored name, so the
// references and the other rows holding it keep working, now encrypted. The
// plaintext is checked against its md5 first; the row is updated once the
// encrypted content is on disk, and only while it still holds that file.
func (s *Service) Encrypt(ctx context.Context, db *cdb.DB, f *cdb.SafeFile) error {
	if s.Ring.Active() == nil {
		return ErrNoKey
	}
	plaintext, err := s.Get(f)
	if err != nil {
		return err
	}
	if f.Encrypted() {
		return nil
	}
	sealed, err := Seal(s.Ring, f.UUID, plaintext)
	if err != nil {
		return err
	}
	_, sumSHA256 := digests(plaintext)
	if err := s.Store.Write(f.UUID, sealed.Content); err != nil {
		return err
	}
	ok, err := db.SetSafeFileEncryption(ctx, f.ID, f.UUID, Version, sealed.KeyID, sealed.WrappedKey, sumSHA256)
	if err != nil || !ok {
		// The row changed meanwhile: put the plaintext back, as the row expects it.
		if restoreErr := s.Store.Write(f.UUID, plaintext); restoreErr != nil {
			return fmt.Errorf("safe: %s: the row changed during the conversion, and the plaintext could not be restored: %w", f.UUID, restoreErr)
		}
		if err != nil {
			return err
		}
		return fmt.Errorf("safe: %s: the row changed during the conversion, left in plaintext", f.UUID)
	}
	return nil
}

// Rewrap wraps a row's file key with the active master key; the file on disk is
// left as it is.
func (s *Service) Rewrap(ctx context.Context, db *cdb.DB, f *cdb.SafeFile) error {
	if !f.Encrypted() {
		return nil
	}
	keyID, wrapped, err := Rewrap(s.Ring, f.UUID, f.EncKeyID, f.WrappedKey)
	if err != nil {
		return err
	}
	ok, err := db.SetSafeFileEncryption(ctx, f.ID, f.UUID, Version, keyID, wrapped, "")
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("safe: %s: the row changed during the rewrap", f.UUID)
	}
	return nil
}
