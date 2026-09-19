package pubsub

import (
	"testing"
)

func TestPubSubBroker(t *testing.T) {
	broker := NewBroker()

	sub1 := make(Subscriber, 10)
	sub2 := make(Subscriber, 10)

	broker.Subscribe("news", sub1)
	broker.Subscribe("news", sub2)
	broker.Subscribe("sports", sub1)

	if count := broker.NumSubscribers("news"); count != 2 {
		t.Fatalf("expected 2 subscribers for news, got %d", count)
	}
	if count := broker.NumSubscribers("sports"); count != 1 {
		t.Fatalf("expected 1 subscriber for sports, got %d", count)
	}

	// Publish to news
	delivered := broker.Publish("news", []byte("breaking news"))
	if delivered != 2 {
		t.Fatalf("expected 2 delivered, got %d", delivered)
	}

	// Unsubscribe sub1
	broker.Unsubscribe("news", sub1)
	if count := broker.NumSubscribers("news"); count != 1 {
		t.Fatalf("expected 1 subscriber after unsubscribe, got %d", count)
	}

	// Unsubscribe all for sub1
	broker.UnsubscribeAll(sub1)
	if count := broker.NumSubscribers("sports"); count != 0 {
		t.Fatalf("expected 0 subscribers for sports after unsubscribe all, got %d", count)
	}

	channels := broker.ActiveChannels()
	if len(channels) != 1 || channels[0] != "news" {
		t.Fatalf("expected only 'news' channel active, got %v", channels)
	}
}
