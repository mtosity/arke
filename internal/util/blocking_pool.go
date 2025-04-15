package util

import (
	"context"
	"sync/atomic"
)

type BlockingPool struct {
	ctx   context.Context
	pool  chan any
	New   func() any
	count atomic.Int32
	limit int32
}

func NewBlockingPool(ctx context.Context, limit int, constructor func() any) *BlockingPool {
	p := make(chan any, limit)
	return &BlockingPool{
		ctx:   ctx,
		pool:  p,
		limit: int32(limit),
		New:   constructor}
}

func (p *BlockingPool) Get() any {
	for {
		select {
		case <-p.ctx.Done():
			return nil
		case e, ok := <-p.pool:
			if ok {
				return e
			}
		default:
			if p.count.Load() < p.limit {
				p.count.Add(1)
				return p.New()
			}
		}
	}
}

func (p *BlockingPool) Put(x any) {
	if x != nil {
		p.pool <- x
	}
}
