package pool

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestPool(t *testing.T) {
	capacity := 10
	blockOpt := NewBlockOption(true)
	pool := NewPool(int32(capacity), blockOpt)
	var wg sync.WaitGroup
	wg.Add(capacity+1)
	work := func(index int) func() error {
		return func() error {
			defer wg.Done()
			for {
				println("this is a debug output: ", fmt.Sprint(index))
				time.Sleep(time.Duration(index+1)*1e9)
				fmt.Println("exit now: ", fmt.Sprint(index))
				return nil
			}
		}
	}
	for i := 1;i <= capacity+1;i++ {
		err := pool.Do(work(i))
		if err != nil {
			t.Error(err)
			return
		}
	}
	wg.Wait()
}
