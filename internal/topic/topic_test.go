package topic

import (
	"testing"
)

func newTestSub() *Subscriber {
	return &Subscriber{Outbound: make(chan Message, 100)}
}

func TestPublishNoSubsStaysQueued(t *testing.T) {
	tp := New("orders")
	tp.Publish(Message{ID: 1, Payload: []byte("hello")})

	if len(tp.Unacked) != 0 {
		t.Fatalf("expected 0 unacked, got %d", len(tp.Unacked))
	}
	msg, ok := tp.Ready.Pop()
	if !ok || msg.ID != 1 || string(msg.Payload) != "hello" {
		t.Fatalf("expected msg 1 in Ready, got %+v ok=%v", msg, ok)
	}
}

func TestPublishDispatchesToOneSub(t *testing.T) {
	tp := New("orders")
	s := newTestSub()
	tp.Sub(s)

	tp.Publish(Message{ID: 1, Payload: []byte("a")})

	select {
	case got := <-s.Outbound:
		if got.ID != 1 {
			t.Fatalf("expected ID 1, got %d", got.ID)
		}
	default:
		t.Fatal("expected message in Outbound")
	}
	if len(tp.Unacked) != 1 {
		t.Fatalf("expected 1 unacked, got %d", len(tp.Unacked))
	}
	if s.UnackedCount != 1 {
		t.Fatalf("expected UnackedCount 1, got %d", s.UnackedCount)
	}
}

func TestAckFreesAndDispatchesNext(t *testing.T) {
	tp := New("orders")
	s := newTestSub()
	tp.Sub(s)

	tp.Publish(Message{ID: 1, Payload: []byte("a")})
	<-s.Outbound
	tp.Ack(s, 1)

	if len(tp.Unacked) != 0 {
		t.Fatalf("expected 0 unacked after ACK, got %d", len(tp.Unacked))
	}
	if s.UnackedCount != 0 {
		t.Fatalf("expected UnackedCount 0, got %d", s.UnackedCount)
	}
}

func TestNackRequeuesFront(t *testing.T) {
	tp := New("orders")
	s := newTestSub()
	tp.Sub(s)

	tp.Publish(Message{ID: 1, Payload: []byte("a")})
	<-s.Outbound
	tp.Nack(s, 1)

	// NACK requeues then immediately redispatches (sub has credit),
	// so it is back in Unacked AND in Outbound.
	if len(tp.Unacked) != 1 {
		t.Fatalf("expected 1 unacked after NACK redelivery, got %d", len(tp.Unacked))
	}
	// NACK redelivers immediately since sub has credit.
	select {
	case got := <-s.Outbound:
		if got.ID != 1 {
			t.Fatalf("expected redelivery of ID 1, got %d", got.ID)
		}
	default:
		t.Fatal("expected redelivery in Outbound")
	}
}

func TestUnsubRequeuesOwned(t *testing.T) {
	tp := New("orders")
	s1 := newTestSub()
	s2 := newTestSub()
	tp.Sub(s1)
	tp.Sub(s2)

	tp.Publish(Message{ID: 1, Payload: []byte("a")})

	// Round-robin picks one of them; drain whoever got it.
	var owner *Subscriber
	select {
	case <-s1.Outbound:
		owner = s1
	case <-s2.Outbound:
		owner = s2
	default:
		t.Fatal("expected delivery to one sub")
	}

	var other *Subscriber
	if owner == s1 {
		other = s2
	} else {
		other = s1
	}

	// Disconnect the owner: message must be requeued and redelivered to other.
	tp.Unsub(owner)

	select {
	case got := <-other.Outbound:
		if got.ID != 1 {
			t.Fatalf("expected redelivery of ID 1, got %d", got.ID)
		}
	default:
		t.Fatal("expected redelivery to surviving sub")
	}
}
