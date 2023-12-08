package types

import (
	"sync"
	"time"
)

type TimedSet struct {
	mu      sync.Mutex
	data    map[int]time.Time
	timeout time.Duration
}

func NewTimedSet(timeout time.Duration) *TimedSet {
	return &TimedSet{
		data:    make(map[int]time.Time),
		timeout: timeout,
	}
}

func (t *TimedSet) Add(item int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 存储或更新值，并重置超时时间
	t.data[item] = time.Time{}

	// 设置定时器，超过指定时间后清空对应值
	time.AfterFunc(t.timeout, func() {
		t.mu.Lock()
		defer t.mu.Unlock()

		// 清空对应值
		delete(t.data, item)
	})
}

func (t *TimedSet) GetData() []int {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 提取map中的所有键（唯一值）
	result := make([]int, 0, len(t.data))
	for key := range t.data {
		result = append(result, key)
	}

	return result
}
