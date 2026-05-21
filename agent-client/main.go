package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "guest-agent/guest-agent/proto"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "agent address")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		log.Fatal("usage: agent-client [-addr host:port] <cmd> [args...]")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewAgentClient(conn)
	stream, err := client.Exec(ctx, &pb.ExecRequest{
		ExecId: randomExecID(),
		Cmd:    args,
	})
	if err != nil {
		log.Fatalf("exec: %v", err)
	}

	exit := 0
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("recv: %v", err)
		}
		if len(resp.Stdout) > 0 {
			os.Stdout.Write(resp.Stdout)
		}
		if len(resp.Stderr) > 0 {
			os.Stderr.Write(resp.Stderr)
		}
		if resp.Done {
			exit = int(resp.ExitCode)
		}
	}
	fmt.Fprintf(os.Stderr, "exit code: %d\n", exit)
	os.Exit(exit)
}

func randomExecID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "cli-" + hex.EncodeToString(b[:])
}