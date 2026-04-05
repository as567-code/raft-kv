package chaos

import (
	"context"
	"fmt"
	"os/exec"
)

// LatencyInjector adds artificial network latency using tc netem.
type LatencyInjector struct {
	containers []string
}

func NewLatencyInjector(containers []string) *LatencyInjector {
	return &LatencyInjector{containers: containers}
}

// AddLatency adds latency to a container's network interface.
func (l *LatencyInjector) AddLatency(ctx context.Context, container string, latencyMs int) error {
	cmd := exec.CommandContext(ctx, "docker", "exec", container,
		"tc", "qdisc", "add", "dev", "eth0", "root", "netem",
		"delay", fmt.Sprintf("%dms", latencyMs))
	return cmd.Run()
}

// RemoveLatency removes latency injection from a container.
func (l *LatencyInjector) RemoveLatency(ctx context.Context, container string) error {
	cmd := exec.CommandContext(ctx, "docker", "exec", container,
		"tc", "qdisc", "del", "dev", "eth0", "root")
	return cmd.Run()
}

// AddLatencyAll adds latency to all containers.
func (l *LatencyInjector) AddLatencyAll(ctx context.Context, latencyMs int) error {
	for _, c := range l.containers {
		if err := l.AddLatency(ctx, c, latencyMs); err != nil {
			return fmt.Errorf("failed to add latency to %s: %w", c, err)
		}
	}
	return nil
}

// RemoveAll removes latency from all containers.
func (l *LatencyInjector) RemoveAll(ctx context.Context) error {
	for _, c := range l.containers {
		l.RemoveLatency(ctx, c) // best effort
	}
	return nil
}
