package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	kvpb "github.com/as567-code/raft-kv/proto/kvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: raft-kv-client <address> <command> [args...]\n")
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  put <key> <value>    Set a key-value pair\n")
		fmt.Fprintf(os.Stderr, "  get <key>            Get a value by key\n")
		fmt.Fprintf(os.Stderr, "  delete <key>         Delete a key\n")
		fmt.Fprintf(os.Stderr, "  scan [prefix]        Scan keys with optional prefix\n")
		os.Exit(1)
	}

	addr := os.Args[1]
	command := os.Args[2]

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	client := kvpb.NewKVServiceClient(conn)

	switch strings.ToLower(command) {
	case "put":
		if len(os.Args) < 5 {
			fmt.Fprintf(os.Stderr, "Usage: put <key> <value>\n")
			os.Exit(1)
		}
		resp, err := client.Put(ctx, &kvpb.PutRequest{
			Key:   os.Args[3],
			Value: os.Args[4],
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Put failed: %v\n", err)
			os.Exit(1)
		}
		if resp.Success {
			fmt.Println("OK")
		} else {
			fmt.Printf("Failed. Leader hint: %s\n", resp.LeaderHint)
		}

	case "get":
		if len(os.Args) < 4 {
			fmt.Fprintf(os.Stderr, "Usage: get <key>\n")
			os.Exit(1)
		}
		readMode := kvpb.ReadMode_READ_LEADER
		if len(os.Args) >= 5 {
			switch strings.ToLower(os.Args[4]) {
			case "follower":
				readMode = kvpb.ReadMode_READ_FOLLOWER
			case "lease":
				readMode = kvpb.ReadMode_READ_LEASE_BASED
			}
		}
		resp, err := client.Get(ctx, &kvpb.GetRequest{
			Key:      os.Args[3],
			ReadMode: readMode,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Get failed: %v\n", err)
			os.Exit(1)
		}
		if resp.Found {
			fmt.Println(resp.Value)
		} else {
			if resp.LeaderHint != "" {
				fmt.Printf("Not found (not leader, try: %s)\n", resp.LeaderHint)
			} else {
				fmt.Println("(not found)")
			}
		}

	case "delete":
		if len(os.Args) < 4 {
			fmt.Fprintf(os.Stderr, "Usage: delete <key>\n")
			os.Exit(1)
		}
		resp, err := client.Delete(ctx, &kvpb.DeleteRequest{Key: os.Args[3]})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Delete failed: %v\n", err)
			os.Exit(1)
		}
		if resp.Success {
			fmt.Println("OK")
		} else {
			fmt.Printf("Failed. Leader hint: %s\n", resp.LeaderHint)
		}

	case "scan":
		prefix := ""
		if len(os.Args) >= 4 {
			prefix = os.Args[3]
		}
		resp, err := client.Scan(ctx, &kvpb.ScanRequest{Prefix: prefix})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Scan failed: %v\n", err)
			os.Exit(1)
		}
		if len(resp.Pairs) == 0 {
			fmt.Println("(empty)")
		} else {
			for k, v := range resp.Pairs {
				fmt.Printf("%s = %s\n", k, v)
			}
		}

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		os.Exit(1)
	}
}
