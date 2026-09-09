package deadhorse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEffectiveCost(t *testing.T) {
	cases := []struct {
		cost, want int64
	}{
		{0, 1},
		{-1, 1},
		{-1000, 1},
		{1, 1},
		{5, 5},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, EffectiveCost(c.cost), "EffectiveCost(%d)", c.cost)
	}
}
