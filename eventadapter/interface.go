package eventadapter

import (
	"context"
	"encoding/json"
)

type Eventer interface {
	Name() string
	Description() string
	Validate() error
	GetOrganisationID() string
}

type EventMetadata struct {
	MessageID   string
	PublishTime int64
	Broker      string
}

type RawHandler func(ctx context.Context, payload []byte, meta EventMetadata) error

type SubscribeHandler[EV Eventer] func(ctx context.Context, ev *EV, meta EventMetadata) error

type SubscribeParams struct {
	MaxHandlers int
}

type Publisher interface {
	Publish(ctx context.Context, ev Eventer, payload []byte) (string, error)
}

type Subscriber interface {
	Subscribe(ctx context.Context, topicName string, handler RawHandler, params SubscribeParams) (func() error, error)
}

type Adapter interface {
	Publisher
	Subscriber
	Name() string
}

func PublishJSON(ctx context.Context, p Publisher, ev Eventer) (string, error) {
	payload, err := json.Marshal(ev)
	if err != nil {
		return "", err
	}
	return p.Publish(ctx, ev, payload)
}

func SubscribeJSON[EV Eventer](ctx context.Context, s Subscriber, topicName string, handler SubscribeHandler[EV], params SubscribeParams) (func() error, error) {
	raw := func(ctx context.Context, payload []byte, meta EventMetadata) error {
		var ev EV
		if err := json.Unmarshal(payload, &ev); err != nil {
			return err
		}
		return handler(ctx, &ev, meta)
	}
	return s.Subscribe(ctx, topicName, raw, params)
}
