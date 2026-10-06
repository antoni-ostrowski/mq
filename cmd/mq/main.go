package main

import (
	"errors"
	"sync"
)

type Message struct {
}

func NewMessage() Message {
	m := Message{}
	return m
}

type Queue struct {
	head int
	tail int
	size int
	arr  []Message
	m    sync.Mutex
}

const QUEUE_BASE_CAP = 100

func NewQueue() *Queue {
	q := Queue{head: 0, tail: 0, size: 0, arr: make([]Message, QUEUE_BASE_CAP)}
	return &q
}

func (q *Queue) Push(n Message) error {
	if q.size >= len(q.arr) {
		return errors.New("queue already at max cap")
	}
	q.arr[q.head] = n
	q.head = q.nextIndex(q.head)
	q.size += 1
	return nil
}

func (q *Queue) Pop() (Message, bool) {
	if q.size == 0 {
		return Message{}, false
	}
	m := q.arr[q.tail]
	// drop reference to message payload so it can be freed by GC, before we overwrrte it
	// which can happen after long time
	q.arr[q.tail] = Message{}
	q.size -= 1
	q.tail = q.nextIndex(q.tail)
	return m, true
}

func (q *Queue) nextIndex(curr_index int) int {
	return (curr_index + 1) % len(q.arr)
}

func main() {

}
