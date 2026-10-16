package safe

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
)

// Version is the format of the files encrypted at rest, stored in
// safe.enc_version: 1, AES-256-GCM with a per-file key wrapped by a master key
// with AES-256-GCM.
const Version = 1

// magic starts an encrypted file, followed by the version byte, the nonce and
// the ciphertext with its tag. The database says whether a file is encrypted;
// the header lets a file found on its own be told apart from a plaintext one.
var magic = []byte("OC3SAFE\x00")

const nonceSize = 12

// ErrOpen tells a file that does not decrypt: altered, moved to another
// record, or wrapped by another key. The message says no more, on purpose.
var ErrOpen = errors.New("safe: the file cannot be decrypted: it was altered, or its key is not the one recorded")

// Sealed is a file encrypted at rest: the content to write to disk, and what the
// database keeps to open it again.
type Sealed struct {
	Content    []byte
	KeyID      string
	WrappedKey []byte
}

// contentAAD and keyAAD bind a ciphertext and a wrapped key to the stored name
// of their file: a ciphertext copied over another file, or a wrapped key
// copied to another row, does not open.
func contentAAD(name string) []byte { return []byte("oc3-safe:v1:content:" + name) }
func keyAAD(name string) []byte     { return []byte("oc3-safe:v1:key:" + name) }

func gcm(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func seal(key, plaintext, aad []byte) ([]byte, error) {
	aead, err := gcm(key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, nonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return aead.Seal(nonce, nonce, plaintext, aad), nil
}

func open(key, sealed, aad []byte) ([]byte, error) {
	if len(sealed) < nonceSize {
		return nil, ErrOpen
	}
	aead, err := gcm(key)
	if err != nil {
		return nil, err
	}
	plaintext, err := aead.Open(nil, sealed[:nonceSize], sealed[nonceSize:], aad)
	if err != nil {
		return nil, ErrOpen
	}
	return plaintext, nil
}

// wipe overwrites a key once used. Best effort: the runtime may have copied it.
func wipe(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

// Seal encrypts the content of the file stored as `name` with a new per-file
// key, wrapped by the active master key.
func Seal(ring *Keyring, name string, plaintext []byte) (*Sealed, error) {
	master := ring.Active()
	if master == nil {
		return nil, ErrNoKey
	}
	fileKey := make([]byte, KeySize)
	if _, err := rand.Read(fileKey); err != nil {
		return nil, err
	}
	defer wipe(fileKey)
	body, err := seal(fileKey, plaintext, contentAAD(name))
	if err != nil {
		return nil, fmt.Errorf("safe: encrypt: %w", err)
	}
	wrapped, err := seal(master.Secret, fileKey, keyAAD(name))
	if err != nil {
		return nil, fmt.Errorf("safe: wrap the file key: %w", err)
	}
	content := make([]byte, 0, len(magic)+1+len(body))
	content = append(content, magic...)
	content = append(content, Version)
	content = append(content, body...)
	return &Sealed{Content: content, KeyID: master.ID, WrappedKey: wrapped}, nil
}

// Open decrypts a file encrypted at rest, with the per-file key the database
// recorded for it.
func Open(ring *Keyring, name string, content []byte, keyID string, wrappedKey []byte) ([]byte, error) {
	master := ring.Get(keyID)
	if master == nil {
		return nil, fmt.Errorf("safe: the master key %q of the file is not in the keyring", keyID)
	}
	if !IsSealed(content) || content[len(magic)] != Version {
		return nil, ErrOpen
	}
	fileKey, err := open(master.Secret, wrappedKey, keyAAD(name))
	if err != nil {
		return nil, err
	}
	defer wipe(fileKey)
	return open(fileKey, content[len(magic)+1:], contentAAD(name))
}

// Rewrap wraps the per-file key of a file with the active master key, the
// content left as it is: what a rotation of the master key changes.
func Rewrap(ring *Keyring, name, keyID string, wrappedKey []byte) (string, []byte, error) {
	master := ring.Get(keyID)
	if master == nil {
		return "", nil, fmt.Errorf("safe: the master key %q of the file is not in the keyring", keyID)
	}
	active := ring.Active()
	if active == nil {
		return "", nil, ErrNoKey
	}
	fileKey, err := open(master.Secret, wrappedKey, keyAAD(name))
	if err != nil {
		return "", nil, err
	}
	defer wipe(fileKey)
	wrapped, err := seal(active.Secret, fileKey, keyAAD(name))
	if err != nil {
		return "", nil, err
	}
	return active.ID, wrapped, nil
}

// IsSealed tells whether a content starts as an encrypted file does.
func IsSealed(content []byte) bool {
	return len(content) > len(magic) && bytes.Equal(content[:len(magic)], magic)
}
