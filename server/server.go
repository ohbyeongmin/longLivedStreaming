package main

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/ohbyeongmin/longLivedStreaming/protos"
	"google.golang.org/grpc"
)

type longlivedServer struct {
	protos.UnimplementedLonglivedServer
	subscribers sync.Map // subscribers is a concurrent map that holds mapping from a client ID to it's subscriber
}

type sub struct {
	stream   protos.Longlived_SubscribeServer
	finished chan<- bool
}

// 클라이언트(전용 고루틴)에서 들어오는 각 구독 요청에 대해 별도의 컨텍스트를 갖는 메소드.
// 요청과 스티리밍 하는데 사용되는 스트림을 받는다.
// 함수가 반환되면 스트림이 닫힌다.
// 클라이언트가 구독 되어 있으면 이 함수는 활성화 상태로 유지되어야 한다.
func (s *longlivedServer) Subscribe(request *protos.Request, stream protos.Longlived_SubscribeServer) error {
	log.Printf("Received subscribe request from ID: %d", request.Id)

	// 구독된 클라이언트에 데이터를 보내려면 해당 스트림에 액세스 해야된다.
	// 스트림이 닫히면 고루틴에 종료 신호를 보내야 한다.
	fin := make(chan bool)
	s.subscribers.Store(request.Id, sub{stream: stream, finished: fin})

	ctx := stream.Context()
	for {
		select {
		case <-fin:
			log.Printf("Closing stream for client ID: %d", request.Id)
			return nil
		case <-ctx.Done():
			log.Printf("Client ID %d has disconnected", request.Id)
			return nil
		}
	}
}

func (s *longlivedServer) mockDataGenerator() {
	log.Println("Starting data generation")
	for {
		time.Sleep(time.Second)

		// A list of clients to unsubscribe in case of error
		var unsubscribe []int32

		// Iterate over all subscribers and send data to each client
		s.subscribers.Range(func(k, v interface{}) bool {
			id, ok := k.(int32)
			if !ok {
				log.Printf("Failed to cast subscriber key: %T", k)
				return false
			}
			sub, ok := v.(sub)
			if !ok {
				log.Printf("Failed to cast subscriber value: %T", v)
			}
			// Send data over the gRPC stream to the client
			if err := sub.stream.Send(&protos.Response{Data: fmt.Sprintf("data mock for: %d", id)}); err != nil {
				log.Printf("Failed to send data to client: %v", err)
				select {
				case sub.finished <- true:
					log.Printf("Unsubscribed client: %d", id)
				default:
					// Default case is to avoid blocking in case client has already unsubscribed
				}
				// In case of error the client would re-subscribe so close the subscriber stream
				unsubscribe = append(unsubscribe, id)
			}
			return true
		})
		for _, id := range unsubscribe {
			s.subscribers.Delete(id)
		}
	}
}

func main() {
	lis, err := net.Listen("tcp", "localhost:7070")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)

	}
	grpcServer := grpc.NewServer([]grpc.ServerOption{}...)

	server := &longlivedServer{}

	go server.mockDataGenerator()

	protos.RegisterLonglivedServer(grpcServer, server)

	log.Printf("Starting server on address %s", lis.Addr().String())

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
}
