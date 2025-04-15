package util

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type Obj struct {
	val int
}

func Test_BlockingPool(t *testing.T) {
	pool := NewBlockingPool(context.Background(), 10, func() any { return Obj{val: 1} })
	o := pool.Get().(Obj)
	assert.Equal(t, 1, o.val)
	o.val = 10
	pool.Put(o)
	p := pool.Get().(Obj)
	assert.Equal(t, 10, p.val)
}

func Test_BlockingPoolCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := NewBlockingPool(ctx, 0, func() any { return Obj{val: 1} })
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	o := pool.Get()
	assert.Nil(t, o)
}

func Test_BlockingPoolLimit(t *testing.T) {
	pool := NewBlockingPool(context.Background(), 1, func() any { return Obj{val: 1} })
	o := pool.Get()
	assert.NotNil(t, o)
	go func() {
		time.Sleep(50 * time.Millisecond)
		pool.Put(o)
	}()
	begin := time.Now()
	p := pool.Get()
	diff := time.Since(begin)
	assert.NotNil(t, p)
	assert.GreaterOrEqual(t, diff.Milliseconds(), int64(50))
}
