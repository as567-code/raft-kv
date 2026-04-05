package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"time"
)

// PartitionResult holds the result of a network partition test.
type PartitionResult struct {
	PartitionType       string  `json:"partition_type"`
	TotalWrites         int     `json:"total_writes"`
	WritesSucceeded     int     `json:"writes_succeeded"`
	WritesFailed        int     `json:"writes_failed"`
	PostHealConsistKeys int     `json:"post_heal_consistent_keys"`
	ConsistencyPercent  float64 `json:"consistency_percentage"`
}

// Partitioner manages network partitions in a Docker cluster.
type Partitioner struct {
	// Container names
	containers []string
}

func NewPartitioner(containers []string) *Partitioner {
	return &Partitioner{containers: containers}
}

// IsolateNode creates a network partition that isolates a container from others.
// Uses iptables inside Docker containers.
func (p *Partitioner) IsolateNode(ctx context.Context, node string, from []string) error {
	for _, target := range from {
		// Block traffic from node to target
		targetIP, err := getContainerIP(ctx, target)
		if err != nil {
			return fmt.Errorf("failed to get IP for %s: %w", target, err)
		}

		cmd := exec.CommandContext(ctx, "docker", "exec", node,
			"iptables", "-A", "OUTPUT", "-d", targetIP, "-j", "DROP")
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to block %s -> %s: %w", node, target, err)
		}

		cmd = exec.CommandContext(ctx, "docker", "exec", node,
			"iptables", "-A", "INPUT", "-s", targetIP, "-j", "DROP")
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to block %s <- %s: %w", node, target, err)
		}
	}
	return nil
}

// HealNode removes all iptables rules from a container.
func (p *Partitioner) HealNode(ctx context.Context, node string) error {
	cmd := exec.CommandContext(ctx, "docker", "exec", node,
		"iptables", "-F")
	return cmd.Run()
}

// HealAll heals all nodes.
func (p *Partitioner) HealAll(ctx context.Context) error {
	for _, c := range p.containers {
		if err := p.HealNode(ctx, c); err != nil {
			return err
		}
	}
	return nil
}

func getContainerIP(ctx context.Context, container string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", "inspect",
		"-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
		container)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return string(out[:len(out)-1]), nil // trim newline
}

// PartitionScenario runs a full partition test scenario.
type PartitionScenario struct {
	Partitioner   *Partitioner
	ClientAddrs   []string // client gRPC addresses
	WriteDuration time.Duration
	WriteRate     int // writes per second
}

func (s *PartitionScenario) ToJSON() ([]byte, error) {
	return json.MarshalIndent(s, "", "  ")
}
