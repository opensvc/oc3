package oidcprovider

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"os"

	"github.com/go-jose/go-jose/v4"
)

// KeySet is the signing keys of the provider: the first one signs, all of them
// are published, so that the tokens signed by a key being retired stay valid
// until they expire.
type KeySet struct {
	signer jose.Signer
	alg    jose.SignatureAlgorithm
	public jose.JSONWebKeySet
}

// LoadKeys reads the private keys of the PEM files, the first one being the
// signing key. A key is RSA (2048 bits or more, RS256) or ECDSA on P-256
// (ES256) or P-384 (ES384).
func LoadKeys(paths []string) (*KeySet, error) {
	if len(paths) == 0 {
		return nil, errors.New("no signing key file")
	}
	keys := make([]crypto.Signer, 0, len(paths))
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("signing key %s: %w", p, err)
		}
		k, err := ParsePrivateKey(b)
		if err != nil {
			return nil, fmt.Errorf("signing key %s: %w", p, err)
		}
		keys = append(keys, k)
	}
	return NewKeySet(keys...)
}

// NewKeySet returns the key set of the keys, the first one signing.
func NewKeySet(keys ...crypto.Signer) (*KeySet, error) {
	if len(keys) == 0 {
		return nil, errors.New("no signing key")
	}
	ks := &KeySet{}
	for i, k := range keys {
		alg, err := keyAlgorithm(k)
		if err != nil {
			return nil, err
		}
		pub := jose.JSONWebKey{Key: k.Public(), Algorithm: string(alg), Use: "sig"}
		thumb, err := pub.Thumbprint(crypto.SHA256)
		if err != nil {
			return nil, fmt.Errorf("key thumbprint: %w", err)
		}
		pub.KeyID = base64.RawURLEncoding.EncodeToString(thumb)
		for _, other := range ks.public.Keys {
			if other.KeyID == pub.KeyID {
				return nil, fmt.Errorf("the key %s is given twice", pub.KeyID)
			}
		}
		ks.public.Keys = append(ks.public.Keys, pub)
		if i == 0 {
			ks.alg = alg
			opts := (&jose.SignerOptions{}).WithType("JWT")
			ks.signer, err = jose.NewSigner(jose.SigningKey{
				Algorithm: alg,
				Key:       jose.JSONWebKey{Key: k, KeyID: pub.KeyID, Algorithm: string(alg)},
			}, opts)
			if err != nil {
				return nil, fmt.Errorf("signer: %w", err)
			}
		}
	}
	return ks, nil
}

// JWKS is the public keys, as the jwks_uri endpoint serves them.
func (ks *KeySet) JWKS() jose.JSONWebKeySet { return ks.public }

// Algorithm is the algorithm the tokens are signed with.
func (ks *KeySet) Algorithm() string { return string(ks.alg) }

func keyAlgorithm(k crypto.Signer) (jose.SignatureAlgorithm, error) {
	switch key := k.(type) {
	case *rsa.PrivateKey:
		if key.N.BitLen() < 2048 {
			return "", fmt.Errorf("RSA key of %d bits, 2048 at least", key.N.BitLen())
		}
		return jose.RS256, nil
	case *ecdsa.PrivateKey:
		switch key.Curve {
		case elliptic.P256():
			return jose.ES256, nil
		case elliptic.P384():
			return jose.ES384, nil
		}
		return "", fmt.Errorf("ECDSA key on %s, P-256 or P-384 expected", key.Curve.Params().Name)
	}
	return "", fmt.Errorf("unsupported key type %T, RSA or ECDSA expected", k)
}

// ParsePrivateKey reads a PEM private key: PKCS#8, PKCS#1 (RSA) or SEC 1 (EC).
func ParsePrivateKey(b []byte) (crypto.Signer, error) {
	for {
		var block *pem.Block
		block, b = pem.Decode(b)
		if block == nil {
			return nil, errors.New("no PEM private key")
		}
		switch block.Type {
		case "PRIVATE KEY":
			k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
			if err != nil {
				return nil, err
			}
			s, ok := k.(crypto.Signer)
			if !ok {
				return nil, fmt.Errorf("unsupported key type %T", k)
			}
			return s, nil
		case "RSA PRIVATE KEY":
			return x509.ParsePKCS1PrivateKey(block.Bytes)
		case "EC PRIVATE KEY":
			return x509.ParseECPrivateKey(block.Bytes)
		}
		// Other blocks, as the EC PARAMETERS openssl writes first, are skipped.
	}
}
