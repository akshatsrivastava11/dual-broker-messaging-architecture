package eventadapter

import "sync/atomic"

type LoadBalancerConfig struct {
	PrimarySplitPercent int32
}

func NewLoadBalancerConfig(primarySplitPercent int32) *LoadBalancerConfig {
	return &LoadBalancerConfig{PrimarySplitPercent: 50}
}

type LoadBalancer struct {
	primary      Adapter
	Secondary    Adapter
	cfg          LoadBalancerConfig
	splitPercent int32
}

func NewLoadBalancer(primary, secondary Adapter, cfg LoadBalancerConfig) *LoadBalancer {
	return &LoadBalancer{
		primary:      primary,
		Secondary:    secondary,
		cfg:          cfg,
		splitPercent: cfg.PrimarySplitPercent,
	}
}
func (lb *LoadBalancer) Name() string {
	return "loadbalancer(" + lb.primary.Name() + "," + lb.Secondary.Name() + ")"
}

func (lb *LoadBalancer) SetSplit(primaryPercent int32) {
	atomic.StoreInt32(&lb.splitPercent, primaryPercent)
}
