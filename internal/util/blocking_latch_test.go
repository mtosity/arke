package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_BlockingLatch(t *testing.T) {
	latch := NewBlockingLatch(2)
	latch.Increment()
	latch.Increment()
	assert.Equal(t, uint(2), latch.Count())
	latch.Decrement()
	assert.Equal(t, uint(1), latch.Count())
	latch.Increment()
	assert.Equal(t, uint(2), latch.Count())
	go func(lt *BlockingLatch) {
		time.Sleep(1000 * time.Millisecond)
		lt.Decrement()
	}(latch)
	latch.Increment()
	assert.Equal(t, uint(2), latch.Count())

	go func(lt *BlockingLatch) {
		time.Sleep(1000 * time.Millisecond)
		lt.Decrement()
		lt.Decrement()
	}(latch)

	latch.WaitForEmpty()
	assert.Equal(t, uint(0), latch.Count())
}
