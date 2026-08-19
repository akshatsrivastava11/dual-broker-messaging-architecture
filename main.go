package main

import (
	"akshat/dual-broker-arch/eventadapter"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type IncidentCreated struct {
	IncidentID     string `json:"incident_id"`
	OrganisationID string `json:"organisation_id"`
	Title          string `json:"title"`
}

func (e IncidentCreated) Name() string              { return "incident.created" }
func (e IncidentCreated) Description() string       { return "Fired when a new incident is created." }
func (e IncidentCreated) GetOrganisationID() string { return e.OrganisationID }
func (e IncidentCreated) Validate() error {
	if e.IncidentID == "" {
		return errors.New("incident_id is required")
	}
	if e.OrganisationID == "" {
		return errors.New("organisation_id is required")
	}
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	projectID := os.Getenv("GCP_PROJECT_ID")
	adapter, err := eventadapter.NewPubsubAdapter(ctx, projectID)
	if err != nil {
		log.Fatal("pubsub adapter: %v", err)
	}

	stopSub, err := eventadapter.SubscribeJSON[IncidentCreated](ctx, adapter, "incident.created",
		func(ctx context.Context, ev *IncidentCreated, meta eventadapter.EventMetadata) error {
			log.Printf("[%s] handling incident.created id=%s", meta.Broker, ev.IncidentID)
			return nil
		}, eventadapter.SubscribeParams{MaxHandlers: 10})
	if err != nil {
		log.Fatal("subscribe: %v", err)
	}
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				i++
				ev := IncidentCreated{IncidentID: fmt.Sprintf("inc_%d", i), OrganisationID: "org_demo", Title: "Something is on fire"}
				if _, err := eventadapter.PublishJSON(ctx, adapter, ev); err != nil {
					log.Printf("publish error: %v", err)
				}
			}
		}
	}()
	<-ctx.Done()
	_ = stopSub()
}
