package broker

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/antoni-ostrowski/mq/internal/topic"
)

type Broker struct {
	nextID atomic.Uint64
	topics map[string]*topic.Topic
	m      sync.RWMutex
}

func New() *Broker {
	return &Broker{topics: make(map[string]*topic.Topic)}
}

func (b *Broker) RegisterTopic(newTopic *topic.Topic) {
	b.m.Lock()
	defer b.m.Unlock()
	b.topics[newTopic.Name] = newTopic
}

func (b *Broker) UnregisterTopic(topicName string) {
	b.m.Lock()
	defer b.m.Unlock()
	delete(b.topics, topicName)
}

func (b *Broker) GetTopic(topicName string) (*topic.Topic, error) {
	b.m.RLock()
	defer b.m.RUnlock()
	t, ok := b.topics[topicName]
	if !ok {
		return nil, errors.New("topic doesnt exist")
	}
	return t, nil
}

func (b *Broker) UnsubAll(s *topic.Subscriber) {
	b.m.RLock()
	topics := make([]*topic.Topic, 0, len(b.topics))
	for _, t := range b.topics {
		topics = append(topics, t)

	}
	b.m.RUnlock()
	for _, t := range topics {
		t.Unsub(s)
	}
}

func (b *Broker) GetNewMsgId() uint64 {
	return b.nextID.Add(1)
}
