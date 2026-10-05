package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/spiridonov/deadhorse"
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

func TestDecodeResult(t *testing.T) {
	entries := []deadhorse.RequestEntry{{Key: "a"}, {Key: "b"}}

	t.Run("ok", func(t *testing.T) {
		results, err := decodeResult("RESULT a|0|5|0 b|1|0|1000", entries)
		require.NoError(t, err)
		require.Len(t, results, 2)
		assert.Equal(t, deadhorse.ResponseEntry{Key: "a", Throttled: false, Remaining: 5, RetryAfter: 0}, results[0])
		assert.Equal(t, deadhorse.ResponseEntry{Key: "b", Throttled: true, Remaining: 0, RetryAfter: 1000 * time.Nanosecond}, results[1])
	})

	t.Run("ERR token", func(t *testing.T) {
		results, err := decodeResult("RESULT a|0|5|0 ERR", entries)
		require.NoError(t, err)
		require.Len(t, results, 2)
		assert.Equal(t, deadhorse.ResponseEntry{Key: "b", Throttled: true, Err: ErrEntryRejected}, results[1])
	})

	t.Run("not a RESULT line", func(t *testing.T) {
		_, err := decodeResult("ERROR boom", entries)
		assert.Error(t, err)
	})

	t.Run("entry count mismatch", func(t *testing.T) {
		_, err := decodeResult("RESULT a|0|5|0", entries)
		assert.Error(t, err)
	})

	t.Run("malformed token", func(t *testing.T) {
		_, err := decodeResult("RESULT a|0|5|0 not|enough|fields", entries)
		assert.Error(t, err)
	})

	t.Run("key mismatch signals a desynced connection instead of mislabeling", func(t *testing.T) {
		// A response whose key doesn't match the request at the same
		// position must be treated as a protocol error, not silently
		// relabeled -- matching is otherwise purely positional, so this is
		// the only thing that would ever catch a desync.
		_, err := decodeResult("RESULT a|0|5|0 wrong-key|1|0|1000", entries)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "wrong-key")
		assert.ErrorContains(t, err, "does not match request key")
	})
}
