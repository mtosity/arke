package util

import "sync"

type BlockingLatch struct {
	count  uint
	max    uint
	lock   *sync.Mutex
	notMax *sync.Cond
}

func NewBlockingLatch(m uint) *BlockingLatch {
	lock := new(sync.Mutex)

	bl := &BlockingLatch{
		count:  0,
		max:    m,
		lock:   lock,
		notMax: sync.NewCond(lock),
	}
	return bl
}

func (bl *BlockingLatch) WaitForEmpty() {
	bl.lock.Lock()
	defer bl.lock.Unlock()
	for {
		if bl.count > 0 {
			bl.notMax.Wait()
		} else {
			return
		}
	}
}

func (bl *BlockingLatch) Count() uint {
	bl.lock.Lock()
	res := bl.count
	bl.lock.Unlock()

	return res
}

func (bl *BlockingLatch) SetMax(m uint) {
	bl.lock.Lock()
	bl.max = m
	bl.lock.Unlock()
}

func (bl *BlockingLatch) GetMax() uint {
	bl.lock.Lock()
	defer bl.lock.Unlock()
	return bl.max
}

/*func (bl *BlockingLatch) inc() {
	bl.lock.Lock()
	bl.count++
	bl.lock.Unlock()
}*/

func (bl *BlockingLatch) Decrement() {
	bl.lock.Lock()
	bl.count--
	bl.notMax.Signal()
	bl.lock.Unlock()
}

func (bl *BlockingLatch) Increment() {
	bl.lock.Lock()
	if bl.count >= bl.max {
		bl.notMax.Wait()
	}
	bl.count++
	bl.lock.Unlock()
}
