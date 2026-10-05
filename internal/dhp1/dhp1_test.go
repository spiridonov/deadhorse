package dhp1

import "testing"

func TestIsKeyDelimiter(t *testing.T) {
	cases := []struct {
		name string
		r    rune
		want bool
	}{
		{"ascii space", ' ', true},
		{"tab", '\t', true},
		{"pipe", '|', true},
		// Any Unicode whitespace, not just the ASCII set -- strings.Fields
		// (what both sides' tokenizers use) splits on all of these too.
		{"vertical tab", '\v', true},
		{"form feed", '\f', true},
		{"non-breaking space", ' ', true},
		{"unicode space separator", ' ', true},
		{"ordinary letter", 'a', false},
		{"digit", '1', false},
		{"colon", ':', false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsKeyDelimiter(c.r); got != c.want {
				t.Errorf("IsKeyDelimiter(%q) = %v, want %v", c.r, got, c.want)
			}
		})
	}
}
