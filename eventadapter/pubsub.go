package eventadapter

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
)

type PubSubAdapter struct {
	client    *pubsub.Client
	projectID string
}

func NewPubsubAdapter(ctx context.Context, projectID string) (*PubSubAdapter, error) {
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("pubsub client: %w", err)
	}
	return &PubSubAdapter{client: client, projectID: projectID}, nil
}

func (p *PubSubAdapter) Name() string {
	return "pubsub"
}
func (p *PubSubAdapter) Close() error {
	return p.client.Close()
}

func (p *PubSubAdapter) ensureTopic(ctx context.Context, topicName string) (*pubsub.Topic, error) {
	topic := p.client.Topic(topicName)
	ok, err := topic.Exists(ctx)
	if err != nil {
		return nil, err
	}
	if ok {
		return topic, nil
	}
	return p.client.CreateTopic(ctx, topicName)
}

func (p *PubSubAdapter) ensureSubscription(ctx context.Context, topic *pubsub.Topic, subID string) (*pubsub.Subscription, error) {
	sub := p.client.Subscription(subID)
	ok, err := sub.Exists(ctx)
	if err != nil {
		return nil, err
	}
	if ok {
		return sub, nil
	}
	return p.client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 10 * time.Minute,
	})
}

func (p *PubSubAdapter) Publish(ctx context.Context, ev Eventer, payload []byte) (string, error) {
	if err := ev.Validate(); err != nil {
		return "", err
	}
	topic, err := p.ensureTopic(ctx, ev.Name())
	if err != nil {
		return "", fmt.Errorf("ensure topic : %w", err)
	}
	result := topic.Publish(ctx, &pubsub.Message{
		Data:       payload,
		Attributes: map[string]string{"organisation_id": ev.GetOrganisationID()},
	})
	return result.Get(ctx)
}

func (p *PubSubAdapter) Subscribe(ctx context.Context, topicName string, handler RawHandler, params SubscribeParams) (func() error, error) {
	topic, err := p.ensureTopic(ctx, topicName)
	if err != nil {
		return nil, fmt.Errorf("ensure topic: %w", err)
	}
	sub, err := p.ensureSubscription(ctx, topic, topicName+"-sub")
	if err != nil {
		return nil, fmt.Errorf("ensure subscription")
	}
	if params.MaxHandlers > 0 {
		sub.ReceiveSettings.MaxOutstandingMessages = params.MaxHandlers
	}
	subCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- sub.Receive(subCtx, func(ctx context.Context, m *pubsub.Message) {
			meta := EventMetadata{MessageID: m.ID, PublishTime: m.PublishTime.UnixNano(), Broker: p.Name()}
			if err := handler(ctx, m.Data, meta); err != nil {
				m.Nack()
				return
			}
			m.Ack()
		})
	}()
	return func() error { cancel(); return <-done }, nil
}

var _ Adapter = (*PubSubAdapter)(nil)
