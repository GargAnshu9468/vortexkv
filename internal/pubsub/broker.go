package pubsub

import (
	"sync"
)

type Subscriber chan []byte

type Broker struct {
	mu          sync.RWMutex
	subscribers map[string]map[Subscriber]struct{}
}

func NewBroker() *Broker {
	return &Broker{
		subscribers: make(map[string]map[Subscriber]struct{}),
	}
}

func (b *Broker) Subscribe(channel string, sub Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.subscribers[channel]; !exists {
		b.subscribers[channel] = make(map[Subscriber]struct{})
	}
	b.subscribers[channel][sub] = struct{}{}
}

func (b *Broker) Unsubscribe(channel string, sub Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if subs, exists := b.subscribers[channel]; exists {
		delete(subs, sub)
		if len(subs) == 0 {
			delete(b.subscribers, channel)
		}
	}
}

func (b *Broker) UnsubscribeAll(sub Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for channel, subs := range b.subscribers {
		delete(subs, sub)
		if len(subs) == 0 {
			delete(b.subscribers, channel)
		}
	}
}

func (b *Broker) Publish(channel string, message []byte) int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	subs, exists := b.subscribers[channel]
	if !exists {
		return 0
	}

	var delivered int64
	for sub := range subs {
		select {
		case sub <- message:
			delivered++
		default:
			// Non-blocking drop if channel buffer full to avoid stalling publisher
		}
	}
	return delivered
}

func (b *Broker) ActiveChannels() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	channels := make([]string, 0, len(b.subscribers))
	for ch := range b.subscribers {
		channels = append(channels, ch)
	}
	return channels
}

func (b *Broker) NumSubscribers(channel string) int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if subs, exists := b.subscribers[channel]; exists {
		return int64(len(subs))
	}
	return 0
}
