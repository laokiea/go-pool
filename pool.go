package pool

import (
	"errors"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	TryLockTimes = 3
)

var (
	ErrPoolClosed = errors.New("pool has been closed")
	ErrPoolDrained = errors.New("pool has been drained")
)

type spin int32

func (s *spin) Lock() {
	backoff := 1
	for {
		for i := 0;i < TryLockTimes;i++ {
			if atomic.CompareAndSwapInt32((*int32)(s), 0, 1) {
				return
			} else {
				for j := 0;j < backoff;j++ {
					runtime.Gosched()
				}
				backoff <<= 1
			}
		}
		backoff = 1
	}
}

func (s *spin) Unlock() {
	if atomic.CompareAndSwapInt32((*int32)(s), 0, 0) {
		return
	}
	atomic.CompareAndSwapInt32((*int32)(s), 1, 0)
}

type Option interface {
	Apply(*Pool)
}

type BlockOption bool
func (o BlockOption) Apply(p *Pool) {
	p.blocking = bool(o)
}
func NewBlockOption(blocking bool) BlockOption {
	return BlockOption(blocking)
}

type logger interface {
	Error(args ...interface{})
}
type LogOption struct {
	logger logger
}
func (o LogOption) Apply(p *Pool) {
	p.logger = o.logger
}
func NewLogOption(logger logger) LogOption {
	return LogOption{logger}
}

type PanicOption func(interface{})
func (o PanicOption) Apply(p *Pool) {
	p.panic = o
}
func NewPanicOption(f func(interface{})) PanicOption {
	return PanicOption(f)
}

type ErrNotifyOption bool
func (o ErrNotifyOption) Apply(p *Pool) {
	p.errNotify = bool(o)
}
func NewErrNotifyOption(notify bool) ErrNotifyOption {
	return ErrNotifyOption(notify)
}

type Log struct {}
func (l Log) Error(args ...interface{}) {
	log.Println(args...)
}

type Pool struct {
	capacity int32
	running  int32
	cond *sync.Cond
	lock sync.Locker
	workers []*Worker
	closed  bool
	options []Option

	// err notify
	ErrChan chan error

	// opt
	blocking bool
	logger logger
	panic  func(interface{})
	errNotify bool
}

func (p *Pool) ReturnWorker(worker *Worker) {
	if worker == nil || p.closed {
		return
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	p.workers = append(p.workers, worker)
	p.cond.Signal()
	return
}

func (p *Pool) Close() {
	if p.closed {
		return
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	p.closed = true
	for _, worker := range p.workers {
		worker.tasks <- nil
	}
	p.cond.Broadcast()
	return
}

func (p *Pool) Do(task func() error) error {
	if p.closed {
		return ErrPoolClosed
	}
	worker := p.getWorker()
	if worker == nil {
		return ErrPoolDrained
	}
	worker.tasks <- task
	return nil
}

func (p *Pool) getWorker() (worker *Worker) {
	p.lock.Lock()
	defer p.lock.Unlock()
	l := len(p.workers)

	if p.closed {
		return
	}

	popWorker := func() {
		worker = p.workers[l-1]
		p.workers[l-1] = nil
		p.workers = p.workers[:l-1]
	}

	if l > 0 {
		popWorker()
		return
	} else if atomic.LoadInt32(&p.running) < atomic.LoadInt32(&p.capacity) {
		worker = NewWorker(p)
		worker.run()
		return
	} else {
		if !p.blocking {
			return nil
		}
		valid := func() bool {
			return len(p.workers) > 0 || atomic.LoadInt32(&p.running) < atomic.LoadInt32(&p.capacity)
		}
		for !valid() {
			p.cond.Wait()
		}
		if p.closed {
			return
		}
		l = len(p.workers)
		if l > 0 {
			popWorker()
			return
		} else {
			worker = NewWorker(p)
			worker.run()
			return
		}
	}
}

type Task func() error
type Worker struct {
	pool *Pool
	tasks chan Task
}

func (worker *Worker) run() {
	atomic.AddInt32(&worker.pool.running, 1)
	go func() {
		defer func() {
			// 直接丢掉
			atomic.AddInt32(&worker.pool.running, -1)
			worker.pool.cond.Signal()
			if v := recover();v != nil {
				if worker.pool.panic != nil {
					worker.pool.panic(v)
				} else {
					worker.pool.logger.Error(v)
				}
			}
		}()
		for task := range worker.tasks {
			if task == nil {
				return
			}
			err := task()
			worker.pool.ReturnWorker(worker)
			if err != nil {
				worker.pool.logger.Error(err.Error())
				// 使用方必须有接受该channel的逻辑
				if worker.pool.errNotify {
					worker.pool.ErrChan <- err
				}
			}
		}
	}()
}

func NewWorker(p *Pool) *Worker {
	worker := &Worker{
		pool: p,
		tasks: make(chan Task, 1),
	}
	return worker
}

func NewPool(capacity int32, options ...Option) *Pool {
	spin := new(spin)
	pool := &Pool{
		capacity: capacity,
		lock:     spin,
		cond:     sync.NewCond(spin),
		blocking: true,
	}
	for _, opt := range options {
		opt.Apply(pool)
	}

	if pool.logger == nil {
		pool.logger = Log{}
	}

	pool.ErrChan = make(chan error, 1)
	pool.workers = make([]*Worker, 0)

	return pool
}
