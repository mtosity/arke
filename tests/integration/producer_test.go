package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	pb "sassoftware.io/convoy/arke/api"
	"google.golang.org/grpc"
)

const (
	arkeAddress = "localhost:50051"
)

func connectConfig() *pb.ConnectionConfiguration {
	connConfig := &pb.ConnectionConfiguration{}
	connConfig.Credentials = &pb.Credentials{Username: "", Password: ""}
	connConfig.Host = "rabbitmq"
	connConfig.Port = 5672
	connConfig.Provider = "amqp091"
	connConfig.Tenant = "/"
	connConfig.PrefetchCount = 5
	// fmt.Println(connConfig)
	return connConfig
}

func produceMessages(conn *grpc.ClientConn, cnt int, message string, addressName string, addressSubject string) error {
	c := pb.NewProducerClient(conn)
	ctx := context.Background()

	// defer c.Disconnect(ctx, &pb.Empty{})

	// message := "test"

	connConfig := connectConfig()

	authResp, err := c.Connect(ctx, connConfig)

	if err != nil {
		return err
	}
	if !authResp.GetSuccess() {
		return errors.New(authResp.GetError().GetMessage())
	}

	address := &pb.Address{Name: addressName, Subject: addressSubject}

	for i := 0; i < cnt; i++ {
		r, err := c.SendMessage(ctx, &pb.Message{Body: []byte(message), Address: address, Persistent: true})
		if err != nil {
			return err
		}
		if !r.GetSuccess() {
			return errors.New(r.GetError().GetMessage())
		}
	}
	return nil
}

func consumeMessages(conn *grpc.ClientConn, messages chan<- *pb.Message, done chan bool, clientConnected chan bool, addressName string, addressSubject string, sourceName string) error {
	// defer close(messages)
	c := pb.NewConsumerClient(conn)

	address := &pb.Address{Name: addressName, Subject: addressSubject}
	source := &pb.Source{Name: sourceName, Address: address}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	connConfig := connectConfig()

	authResp, err := c.Connect(ctx, connConfig)

	if err != nil {
		log.Panicf("could not authenticate: %v", err)
	}
	if !authResp.GetSuccess() {
		log.Panicf("could not authenticate: %v", authResp.GetError().GetMessage())
	}
	clientConnected <- true

	stream, err := c.Subscribe(ctx, source)
	if err != nil {
		log.Panicf("could not subscribe: %v", err)
	}

	err = nil

	for {
		message, err := stream.Recv()
		if err == io.EOF {
			// fmt.Printf("err: %v", err)
			return err
		}
		if err != nil {
			// fmt.Printf("err: %v", err)
			break
		}
		messages <- message
		// log.Printf("consumed message %s", message.GetUuid())
		_, err = c.AckMessage(ctx, message)
		if err != nil {
			fmt.Println(err)
			break
		}
		// log.Printf("acked message %s", message.GetUuid())
	}
	done <- true
	return err
}

func connect() *grpc.ClientConn {
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()
	conn, err := grpc.Dial(arkeAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("client did not connect: %v", err)
	}
	return conn
}

func TestProduceSingleConsumeSingle(t *testing.T) {
	producerConnection := connect()
	expectedMessageCount := 1
	defer producerConnection.Close()

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	consumerConnection := connect()
	defer consumerConnection.Close()
	go consumeMessages(consumerConnection, messages, done, clientConnected, "amq.topic", "sas.test.proxy.TPSCS", "sas.test.proxy.TPSCS.Consumer")
	<-clientConnected

	err := produceMessages(producerConnection, expectedMessageCount, "mymessage", "amq.topic", "sas.test.proxy.TPSCS")
	assert.Nil(t, err)

	msgCount := 0

	for start := time.Now(); time.Since(start) < 1*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			break
		case <-time.After(1 * time.Second):
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestProduceManyConsumeMany(t *testing.T) {
	producerConnection := connect()
	expectedMessageCount := 30
	defer producerConnection.Close()

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)

	consumerConnection := connect()
	defer consumerConnection.Close()
	go consumeMessages(consumerConnection, messages, done, clientConnected, "amq.topic", "sas.test.proxy.TPMCM4", "sas.test.proxy.TPMCM4.Consumer")
	<-clientConnected

	err := produceMessages(producerConnection, expectedMessageCount, "mymessage", "amq.topic", "sas.test.proxy.TPMCM4")
	assert.Nil(t, err)

	msgCount := 0

	for start := time.Now(); time.Since(start) < 1*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			break
		case <-time.After(1 * time.Second):
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestProduceFailsWithoutConnect(t *testing.T) {
	conn := connect()
	c := pb.NewProducerClient(conn)
	ctx := context.Background()
	defer c.Disconnect(ctx, &pb.Empty{})
	defer conn.Close()

	address := &pb.Address{Name: "amq.topic", Subject: "sas.test.proxy.TPFWC"}

	r, err := c.SendMessage(ctx, &pb.Message{Body: []byte("message"), Address: address, Persistent: true})
	assert.NotNil(t, err)
	assert.Nil(t, r)
	assert.Contains(t, err.Error(), "could not retrieve broker details")
}

func TestAckFailsWithoutConnect(t *testing.T) {
	conn := connect()
	c := pb.NewConsumerClient(conn)
	ctx := context.Background()
	defer c.Disconnect(ctx, &pb.Empty{})
	defer conn.Close()

	address := &pb.Address{Name: "amq.topic", Subject: "sas.test.proxy.TAFWC"}

	r, err := c.AckMessage(ctx, &pb.Message{Body: []byte("message"), Address: address, Persistent: true})
	assert.NotNil(t, err)
	assert.Nil(t, r)
	assert.Contains(t, err.Error(), "could not retrieve broker details")
}

func TestNackFailsWithoutConnect(t *testing.T) {
	conn := connect()
	c := pb.NewConsumerClient(conn)
	ctx := context.Background()
	defer c.Disconnect(ctx, &pb.Empty{})
	defer conn.Close()

	address := &pb.Address{Name: "amq.topic", Subject: "sas.test.proxy.TAFWC"}

	r, err := c.NackMessage(ctx, &pb.Message{Body: []byte("message"), Address: address, Persistent: true})
	assert.NotNil(t, err)
	assert.Nil(t, r)
	assert.Contains(t, err.Error(), "could not retrieve broker details")
}

func TestConsumerDisconnectOKWithoutConnect(t *testing.T) {
	conn := connect()
	c := pb.NewConsumerClient(conn)
	ctx := context.Background()
	defer c.Disconnect(ctx, &pb.Empty{})
	defer conn.Close()

	r, err := c.Disconnect(ctx, &pb.Empty{})
	assert.Nil(t, err)
	assert.NotNil(t, r)
}

func TestProducerDisconnectOKWithoutConnect(t *testing.T) {
	conn := connect()
	c := pb.NewProducerClient(conn)
	ctx := context.Background()
	defer c.Disconnect(ctx, &pb.Empty{})
	defer conn.Close()

	r, err := c.Disconnect(ctx, &pb.Empty{})
	assert.Nil(t, err)
	assert.NotNil(t, r)
}
