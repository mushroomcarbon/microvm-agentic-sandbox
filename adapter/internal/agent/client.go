package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "guest-agent/guest-agent/proto"
)

// Client wraps a gRPC connection to a single guest agent.
type Client struct {
	conn   *grpc.ClientConn
	client pb.AgentClient
}

// ExecRequest is the adapter-level representation of an Exec call.
type ExecRequest struct {
	ExecID  string
	Cmd     []string
	Env     map[string]string
	Workdir string
}

// ExecChunk is one streamed piece of output, or a final exit-code marker.
type ExecChunk struct {
	Stdout   []byte
	Stderr   []byte
	Done     bool
	ExitCode int32
}

// Dial opens a gRPC connection to the guest agent at addr (e.g. "10.42.0.12:50051").
func Dial(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial agent: %w", err)
	}
	return &Client{conn: conn, client: pb.NewAgentClient(conn)}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

// Ping verifies the agent is responding by exec'ing /bin/true with a synthetic
// exec_id. Will be replaced with a dedicated Health RPC once we add one.
func (c *Client) Ping(ctx context.Context) error {
	stream, err := c.client.Exec(ctx, &pb.ExecRequest{
		ExecId: "ping-" + randomHex(8),
		Cmd:    []string{"/bin/true"},
	})
	if err != nil {
		return err
	}
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// Exec runs a command and streams output chunks back through a channel. The
// channel closes when the command finishes or the stream errors. The caller
// must supply ExecID in req, which is the handle for any follow-up StdinWrite
// or Signal calls.
func (c *Client) Exec(ctx context.Context, req ExecRequest) (<-chan ExecChunk, error) {
	if req.ExecID == "" {
		return nil, fmt.Errorf("ExecID is required")
	}
	stream, err := c.client.Exec(ctx, &pb.ExecRequest{
		ExecId:  req.ExecID,
		Cmd:     req.Cmd,
		Env:     req.Env,
		Workdir: req.Workdir,
	})
	if err != nil {
		return nil, fmt.Errorf("exec: %w", err)
	}

	out := make(chan ExecChunk, 16)
	go func() {
		defer close(out)
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				return
			}
			out <- ExecChunk{
				Stdout:   resp.Stdout,
				Stderr:   resp.Stderr,
				Done:     resp.Done,
				ExitCode: resp.ExitCode,
			}
		}
	}()
	return out, nil
}

// StdinWrite sends bytes to the stdin of the named running exec. If eof is
// true the agent also closes the stdin pipe after writing, sending EOF to the
// process. Returns NotFound if no exec is registered under execID.
func (c *Client) StdinWrite(ctx context.Context, execID string, data []byte, eof bool) error {
	_, err := c.client.StdinWrite(ctx, &pb.StdinWriteRequest{
		ExecId: execID,
		Data:   data,
		Eof:    eof,
	})
	return err
}

// Signal delivers a POSIX signal to the named running exec.
func (c *Client) Signal(ctx context.Context, execID string, signo int32) error {
	_, err := c.client.Signal(ctx, &pb.SignalRequest{
		ExecId: execID,
		Signal: signo,
	})
	return err
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}