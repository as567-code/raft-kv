package chaos

import (
	"context"
	"fmt"
	"os/exec"
	"time"
)

// CrashInjector manages node crash/restart scenarios.
type CrashInjector struct {
	containers []string
}

func NewCrashInjector(containers []string) *CrashInjector {
	return &CrashInjector{containers: containers}
}

// StopNode gracefully stops a Docker container.
func (c *CrashInjector) StopNode(ctx context.Context, container string) error {
	cmd := exec.CommandContext(ctx, "docker", "stop", "-t", "1", container)
	return cmd.Run()
}

// KillNode forcefully kills a Docker container (simulates crash).
func (c *CrashInjector) KillNode(ctx context.Context, container string) error {
	cmd := exec.CommandContext(ctx, "docker", "kill", container)
	return cmd.Run()
}

// RestartNode restarts a Docker container.
func (c *CrashInjector) RestartNode(ctx context.Context, container string) error {
	cmd := exec.CommandContext(ctx, "docker", "start", container)
	return cmd.Run()
}

// CrashAndRecover kills a node and waits before restarting it.
func (c *CrashInjector) CrashAndRecover(ctx context.Context, container string, downtime time.Duration) error {
	if err := c.KillNode(ctx, container); err != nil {
		return fmt.Errorf("kill failed: %w", err)
	}

	select {
	case <-time.After(downtime):
	case <-ctx.Done():
		return ctx.Err()
	}

	if err := c.RestartNode(ctx, container); err != nil {
		return fmt.Errorf("restart failed: %w", err)
	}
	return nil
}
