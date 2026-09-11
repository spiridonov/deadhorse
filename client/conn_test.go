package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateKey(t *testing.T) {
	cases := []struct {
		name string
		key  string
		ok   bool
	}{
		{"empty", "", false},
		{"plain key", "user:42:writes", true},
		{"ascii space", "bad key", false},
		{"tab", "bad\tkey", false},
		{"pipe", "bad|key", false},
		// The wire protocol's own tokenizers (strings.Fields, on both the
		// server and this package's decodeResult) split on any Unicode
		// whitespace, not just the ASCII set -- validateKey must reject
		// the same set, or a key like these would pass here only to get
		// split into two wire tokens later, desyncing the whole batch.
		{"vertical tab", "bad\vkey", false},
		{"form feed", "bad\fkey", false},
		{"non-breaking space", "bad key", false},
		{"unicode space separator", "bad key", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateKey(c.key)
			if c.ok {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
