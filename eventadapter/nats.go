package eventadapter

import (
	"context"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type NatsAdapter struct {
	nc *nats.Conn
	js jetstream.JetStream
}

func NewNATSAdapter(ctx context.Context, url string) (*NatsAdapter, error) {
	nc, err := nats.Connect(url, nats.MaxReconnects(-1))
	if err != nil {
		return nil, fmt.Errorf("nats conncted : %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream: %w", err)
	}
	return &NatsAdapter{
		nc: nc,
		js: js,
	}, nil
}

func (n *NatsAdapter) Name() string {
	return "nats"
}

func (n *NatsAdapter) Close() { n.nc.Close() }

func jsSafeName(topicName string) string { return strings.ReplaceAll(topicName, ".", "_") }

func (n *NatsAdapter) ensureStream(ctx context.Context, topicName string) (jetstream.Stream, error) {
	name := jsSafeName(topicName)
	if stream, err := n.js.Stream(ctx, name); err == nil {
		return stream, nil
	}
	return n.js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      name,
		Subjects:  []string{topicName},
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
	})
}

func (n *NatsAdapter) Publish(ctx context.Context, ev Eventer, payload []byte) (string, error) {
	if err := ev.Validate(); err != nil {
		return "", err
	}
	if _, err := n.ensureStream(ctx, ev.Name()); err != nil {
		return "", fmt.Errorf("ensure stream: %w", err)
	}
	msg := &nats.Msg{Subject: ev.Name(), Data: payload, Header: nats.Header{"organisation_id": []string{ev.GetOrganisationID()}}}
	ack, err := n.js.PublishMsg(ctx, msg)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s-%d", ack.Stream, ack.Sequence), nil
}

func (n *NatsAdapter) Subscribe(ctx context.Context, topicName string, handler RawHandler, params SubscribeParams) (func() error, error) {
	stream, err := n.ensureStream(ctx, topicName)
	if err != nil {
		return nil, err
	}
	maxAckPending := params.MaxHandlers
	if maxAckPending <= 0 {
		maxAckPending = 1
	}
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       jsSafeName(topicName) + "_worker",
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: maxAckPending,
	})
	if err != nil {
		return nil, err
	}

	consCtx, err := cons.Consume(func(msg jetstream.Msg) {
		meta, mErr := msg.Metadata()
		emeta := EventMetadata{Broker: n.Name()}
		if mErr == nil {
			emeta.MessageID = fmt.Sprintf("%d", meta.Sequence.Stream)
			emeta.PublishTime = meta.Timestamp.UnixNano()
		}
		if err := handler(context.Background(), msg.Data(), emeta); err != nil {
			_ = msg.Nak()
			return
		}
		_ = msg.Ack()
	})
	if err != nil {
		return nil, err
	}
	return func() error { consCtx.Stop(); return nil }, nil
}

var _ Adapter = (*NatsAdapter)(nil)
