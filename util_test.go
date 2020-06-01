package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToken(t *testing.T) {
	assert := assert.New(t)

	tres := parseTres("cpu=1,mem=5680M,node=1,billing=1,gres/gpu=1")

	assert.Equal(1, tres.CPU)
	assert.Equal(1, tres.Node)
	assert.Equal(1, tres.Billing)
	assert.Equal(1, tres.GresGpu)
	assert.Equal(uint64(5680000000), tres.Memory)

	tres = parseTres("cpu=32,mem=177.50G,node=1,billing=32,gres/gpu=2")

	assert.Equal(32, tres.CPU)
	assert.Equal(1, tres.Node)
	assert.Equal(32, tres.Billing)
	assert.Equal(2, tres.GresGpu)
	assert.Equal(uint64(177500000000), tres.Memory)

	tres = parseTres("cpu=48,mem=278400M,node=4,billing=48")

	assert.Equal(48, tres.CPU)
	assert.Equal(4, tres.Node)
	assert.Equal(48, tres.Billing)
	assert.Equal(0, tres.GresGpu)
	assert.Equal(uint64(278400000000), tres.Memory)
}
