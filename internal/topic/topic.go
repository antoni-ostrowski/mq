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
	nextSub int
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
	if len(t.Subs) == 0 {
		t.nextSub = 0
	} else if t.nextSub >= len(t.Subs) {
		t.nextSub = 0
	}
	// Requeue in-flight messages owned by s so they are redelivered.
	owned := make([]UnackedEntry, 0)
	for id, e := range t.Unacked {
		if e.Owner == s {
			owned = append(owned, e)
			delete(t.Unacked, id)
		}
	}
	if len(owned) > 0 {
		if s.UnackedCount >= len(owned) {
			s.UnackedCount -= len(owned)
		} else {
			s.UnackedCount = 0
		}
		slices.SortFunc(owned, func(a, b UnackedEntry) int {
			switch {
			case a.Msg.ID < b.Msg.ID:
				return -1
			case a.Msg.ID > b.Msg.ID:
				return 1
			default:
				return 0
			}
		})
		// PushFront in reverse so smallest ID ends at front.
		for i := len(owned) - 1; i >= 0; i-- {
			owned[i].Redeliveries++
			t.Ready.PushFront(owned[i].Msg)
		}
		t.dispatchLocked()
	}
}
func (t *Topic) Publish(msg Message) {
	t.m.Lock()
	defer t.m.Unlock()
	msg.Topic = t.Name
	t.Ready.Push(msg)
	t.dispatchLocked()
}

// finds candidate for handling new msg, and sends msg to him, tracking msg in unacked map
func (t *Topic) dispatchLocked() {
	for len(t.Subs) > 0 {
		var sub *Subscriber
		for range len(t.Subs) {
			t.nextSub = (t.nextSub + 1) % len(t.Subs)
			s := t.Subs[t.nextSub]
			if s.UnackedCount >= 100 {
				continue
			}
			if len(s.Outbound) >= cap(s.Outbound) {
				continue
			}
			sub = s
			break
		}
		if sub == nil {
			return
		} // nobody can take one

		// 2. now pop once
		msg, ok := t.Ready.Pop()
		if !ok {
			return
		}

		// 3. non-blocking send, still need default for the race
		// between the len check and now
		select {
		case sub.Outbound <- msg:
			sub.UnackedCount++
			t.Unacked[msg.ID] = UnackedEntry{Msg: msg, Owner: sub}
		default:
			t.Ready.PushFront(msg) // needs front insert, else order breaks
			return
		}
	}
}
func (t *Topic) Ack(s *Subscriber, id uint64) {
	t.m.Lock()
	defer t.m.Unlock()
	a, ok := t.Unacked[id]
	if !ok {
		return
	}
	if a.Owner != s {
		return
	}
	delete(t.Unacked, id)
	if a.Owner.UnackedCount > 0 {
		a.Owner.UnackedCount--
	}
	t.dispatchLocked()
}

func (t *Topic) Nack(s *Subscriber, id uint64) {
	t.m.Lock()
	defer t.m.Unlock()
	a, ok := t.Unacked[id]
	if !ok {
		return
	}
	if a.Owner != s {
		return
	}
	delete(t.Unacked, id)
	if a.Owner.UnackedCount > 0 {
		a.Owner.UnackedCount--
	}
	a.Redeliveries++
	t.Ready.PushFront(a.Msg)
	t.dispatchLocked()
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
	Topic   string
}
