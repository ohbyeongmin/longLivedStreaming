package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/ohbyeongmin/longLivedStreaming/protos"
	"google.golang.org/grpc"
)

type longlivedClient struct {
	client protos.LonglivedClient // client is the long lived gRPC client
	conn   *grpc.ClientConn       // conn is the client gRPC connection
	id     int32                  // id is the client ID used for subscribing
}

func (c *longlivedClient) subscribe() (protos.Longlived_SubscribeClient, error) {
	log.Printf("Subscribing client ID: %d", c.id)
	return c.client.Subscribe(context.Background(), &protos.Request{Id: c.id})
}

func (c *longlivedClient) start() {
	var err error
	// 구독 후 서버에서 클라이언트로 데이터를 스트리밍 하는데 사용
	var stream protos.Longlived_SubscribeClient
	for {
		if stream == nil {
			if stream, err = c.subscribe(); err != nil {
				log.Printf("Failed to subscribe: %v", err)
				c.sleep()
				// 실패 할 경우 다시 시도
				continue
			}
		}
		response, err := stream.Recv()
		if err != nil {
			log.Printf("Failed to recevied message: %v", err)
			stream = nil
			c.sleep()
			// 실패 할 경우 다시 시도
			continue
		}
		log.Printf("Client ID %d got response: %q", c.id, response.Data)
	}
}

func (c *longlivedClient) sleep() {
	time.Sleep(time.Second * 5)
}

func mkLonglivedClient(id int32) (*longlivedClient, error) {
	conn, err := mkConnection()
	if err != nil {
		return nil, err
	}
	return &longlivedClient{
		client: protos.NewLonglivedClient(conn),
		conn:   conn,
		id:     id,
	}, nil

}

func mkConnection() (*grpc.ClientConn, error) {
	return grpc.Dial("localhost:7070", []grpc.DialOption{grpc.WithInsecure(), grpc.WithBlock()}...)
}

func main() {
	// Creating multiple clients and start receiving data
	var wg sync.WaitGroup

	for i := 1; i <= 10; i++ {
		wg.Add(1)
		client, err := mkLonglivedClient(int32(i))
		if err != nil {
			log.Fatal(err)
		}
		go client.start()
		time.Sleep(time.Second * 2)
	}
	// The wait group purpose is to avoid exiting, the clients do not exit
	wg.Wait()
}
