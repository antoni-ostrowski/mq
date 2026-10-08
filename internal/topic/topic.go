package topic

import (
	"net"
	"slices"
	"sync"

	"github.com/antoni-ostrowski/mq/internal/queue"
)

type Topic struct {
	Name    string
	Ready   *queue.Queue[Message]
	Unacked map[uint64]UnackedEntry
	Subs    []*Subscriber
	m       sync.Mutex
}

func New(n string) *Topic {
	return &Topic{Name: n, Ready: queue.NewQueue[Message](), Unacked: make(map[uint64]UnackedEntry)}
}

func (t *Topic) Sub(s *Subscriber) {
	t.m.Lock()
	defer t.m.Unlock()
	if slices.Contains(t.Subs, s) {
		return
	}
	t.Subs = append(t.Subs, s)
}
func (t *Topic) Unsub(s *Subscriber) {
	t.m.Lock()
	defer t.m.Unlock()
	for i, sub := range t.Subs {
		// swap-remove, unordered
		if sub == s {
			lastIdx := len(t.Subs) - 1
			last := t.Subs[lastIdx]
			t.Subs[i] = last
			t.Subs[lastIdx] = nil // drop ref so conn can GC
			t.Subs = t.Subs[:lastIdx]
			break
		}
	}

}

type UnackedEntry struct {
	Msg          Message
	Owner        *Subscriber
	Redeliveries int
}

type Subscriber struct {
	Conn         net.Conn
	Outbound     chan Message
	UnackedCount int
}

func NewSubscriber(conn net.Conn) *Subscriber {
	return &Subscriber{Conn: conn, Outbound: make(chan Message, 100), UnackedCount: 0}
}

type Message struct {
	ID      uint64
	Payload []byte
}
