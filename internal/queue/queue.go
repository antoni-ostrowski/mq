package queue

import (
	"errors"
	"sync"
)

type Queue[T any] struct {
	head int
	tail int
	size int
	arr  []T
	m    sync.Mutex
}

const QUEUE_BASE_CAP = 100

func NewQueue[T any]() *Queue[T] {
	q := Queue[T]{head: 0, tail: 0, size: 0, arr: make([]T, QUEUE_BASE_CAP)}
	return &q
}

func (q *Queue[T]) Push(n T) error {
	q.m.Lock()
	defer q.m.Unlock()
	if q.size >= len(q.arr) {
		return errors.New("queue already at max cap")
	}
	q.arr[q.head] = n
	q.head = q.nextIndex(q.head)
	q.size += 1
	return nil
}

func (q *Queue[T]) Pop() (T, bool) {
	var zeroT T
	q.m.Lock()
	defer q.m.Unlock()
	if q.size == 0 {
		return zeroT, false
	}
	m := q.arr[q.tail]
	// drop reference to message payload so it can be freed by GC, before we overwrrte it
	// which can happen after long time
	q.arr[q.tail] = zeroT
	q.size -= 1
	q.tail = q.nextIndex(q.tail)
	return m, true
}

func (q *Queue[T]) PushFront(msg T) error {
	q.m.Lock()
	defer q.m.Unlock()
	if q.size >= len(q.arr) {
		return errors.New("queue already at max cap")
	}
	// add len of arr to properly handle negative index
	q.tail = (q.tail - 1 + len(q.arr)) % len(q.arr)
	q.arr[q.tail] = msg
	q.size++
	return nil
}

func (q *Queue[T]) nextIndex(curr_index int) int {
	return (curr_index + 1) % len(q.arr)
}
