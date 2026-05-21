package main

import (
	"log"
	"net"

	"google.golang.org/grpc"
	pb "guest-agent/guest-agent/proto"
)

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterAgentServer(s, newAgentServer())

	log.Println("guest agent listening on :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
