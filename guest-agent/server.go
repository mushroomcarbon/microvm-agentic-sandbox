package main

import (
	"context"
	"io"
	"log"
	"os"
	"os/exec"
	"sync"
	"syscall"

	"github.com/fsnotify/fsnotify"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "guest-agent/guest-agent/proto"
)

// agentServer keeps a registry of running execs so that StdinWrite and Signal
// can route to the right child process by exec_id.
type agentServer struct {
	pb.UnimplementedAgentServer

	mu    sync.RWMutex
	execs map[string]*runningExec
}

// runningExec is the agent-side handle on a single in-flight exec.
type runningExec struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	cancel context.CancelFunc
}

func newAgentServer() *agentServer {
	return &agentServer{
		execs: make(map[string]*runningExec),
	}
}

func (s *agentServer) registerExec(execID string, ex *runningExec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.execs[execID]; exists {
		return status.Errorf(codes.AlreadyExists, "exec %s already running", execID)
	}
	s.execs[execID] = ex
	return nil
}

func (s *agentServer) unregisterExec(execID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.execs, execID)
}

func (s *agentServer) getExec(execID string) (*runningExec, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ex, ok := s.execs[execID]
	return ex, ok
}

func (s *agentServer) Exec(req *pb.ExecRequest, stream pb.Agent_ExecServer) error {
	if req.ExecId == "" {
		return status.Error(codes.InvalidArgument, "exec_id is required")
	}
	if len(req.Cmd) == 0 {
		return status.Error(codes.InvalidArgument, "cmd is empty")
	}

	// Use a cancellable context so closing the stream (or registry.cancel())
	// reliably terminates the child.
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	cmd := exec.CommandContext(ctx, req.Cmd[0], req.Cmd[1:]...)
	cmd.Dir = req.Workdir
	for k, v := range req.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return status.Errorf(codes.Internal, "stdin pipe: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return status.Errorf(codes.Internal, "stdout pipe: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return status.Errorf(codes.Internal, "stderr pipe: %v", err)
	}

	if err := cmd.Start(); err != nil {
		return status.Errorf(codes.Internal, "start: %v", err)
	}

	if err := s.registerExec(req.ExecId, &runningExec{
		cmd:    cmd,
		stdin:  stdin,
		cancel: cancel,
	}); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return err
	}
	defer s.unregisterExec(req.ExecId)

	// Pump stdout and stderr in parallel. The original implementation read
	// stdout to completion before reading stderr, which deadlocks when stderr
	// fills its 64KB pipe buffer while stdout is still being drained.
	var sendMu sync.Mutex
	send := func(resp *pb.ExecResponse) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(resp)
	}

	pump := func(r io.Reader, isErr bool, done chan<- error) {
		buf := make([]byte, 4096)
		for {
			n, err := r.Read(buf)
			if n > 0 {
				chunk := make([]byte, n)
				copy(chunk, buf[:n])
				resp := &pb.ExecResponse{}
				if isErr {
					resp.Stderr = chunk
				} else {
					resp.Stdout = chunk
				}
				if sendErr := send(resp); sendErr != nil {
					done <- sendErr
					return
				}
			}
			if err != nil {
				if err == io.EOF {
					done <- nil
				} else {
					done <- err
				}
				return
			}
		}
	}

	outDone := make(chan error, 1)
	errDone := make(chan error, 1)
	go pump(stdout, false, outDone)
	go pump(stderr, true, errDone)

	if err := <-outDone; err != nil {
		log.Printf("exec %s stdout pump: %v", req.ExecId, err)
	}
	if err := <-errDone; err != nil {
		log.Printf("exec %s stderr pump: %v", req.ExecId, err)
	}

	exitCode := 0
	if err := cmd.Wait(); err != nil {
		if ex, ok := err.(*exec.ExitError); ok {
			// Use the 128+signum shell convention for signaled processes so
			// callers see 143 for SIGTERM, 137 for SIGKILL, etc. Go's
			// ExitCode() returns -1 for signaled processes, which loses the
			// information about which signal killed it.
			if ws, ok := ex.ProcessState.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
				exitCode = 128 + int(ws.Signal())
			} else {
				exitCode = ex.ExitCode()
			}
		} else {
			log.Printf("exec %s wait: %v", req.ExecId, err)
		}
	}
	return send(&pb.ExecResponse{ExitCode: int32(exitCode), Done: true})
}

func (s *agentServer) StdinWrite(_ context.Context, req *pb.StdinWriteRequest) (*pb.StdinWriteResponse, error) {
	if req.ExecId == "" {
		return nil, status.Error(codes.InvalidArgument, "exec_id is required")
	}
	ex, ok := s.getExec(req.ExecId)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "exec %s not found", req.ExecId)
	}
	if len(req.Data) > 0 {
		if _, err := ex.stdin.Write(req.Data); err != nil {
			return nil, status.Errorf(codes.Internal, "write stdin: %v", err)
		}
	}
	if req.Eof {
		if err := ex.stdin.Close(); err != nil {
			return nil, status.Errorf(codes.Internal, "close stdin: %v", err)
		}
	}
	return &pb.StdinWriteResponse{}, nil
}

func (s *agentServer) Signal(_ context.Context, req *pb.SignalRequest) (*pb.SignalResponse, error) {
	if req.ExecId == "" {
		return nil, status.Error(codes.InvalidArgument, "exec_id is required")
	}
	ex, ok := s.getExec(req.ExecId)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "exec %s not found", req.ExecId)
	}
	if ex.cmd.Process == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "process not started")
	}
	if err := ex.cmd.Process.Signal(syscall.Signal(req.Signal)); err != nil {
		return nil, status.Errorf(codes.Internal, "signal: %v", err)
	}
	return &pb.SignalResponse{}, nil
}

func (s *agentServer) FileRead(_ context.Context, req *pb.FileReadRequest) (*pb.FileReadResponse, error) {
	data, err := os.ReadFile(req.Path)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read: %v", err)
	}
	return &pb.FileReadResponse{Data: data}, nil
}

func (s *agentServer) FileWrite(_ context.Context, req *pb.FileWriteRequest) (*pb.FileWriteResponse, error) {
	mode := os.FileMode(req.Mode)
	if mode == 0 {
		mode = 0644
	}
	if err := os.WriteFile(req.Path, req.Data, mode); err != nil {
		return nil, status.Errorf(codes.Internal, "write: %v", err)
	}
	return &pb.FileWriteResponse{}, nil
}

func (s *agentServer) Watch(req *pb.WatchRequest, stream pb.Agent_WatchServer) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return status.Errorf(codes.Internal, "watcher: %v", err)
	}
	defer watcher.Close()

	if err := watcher.Add(req.Path); err != nil {
		return status.Errorf(codes.Internal, "watch add: %v", err)
	}

	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if err := stream.Send(&pb.WatchEvent{Path: event.Name, Op: event.Op.String()}); err != nil {
				return err
			}
		case err := <-watcher.Errors:
			return status.Errorf(codes.Internal, "watch error: %v", err)
		case <-stream.Context().Done():
			return nil
		}
	}
}