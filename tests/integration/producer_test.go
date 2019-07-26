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
	connConfig.Credentials = &pb.Credentials{Username: "guest", Password: "guest"}
	connConfig.Host = "rabbitmq"
	connConfig.Port = 5672
	connConfig.Provider = "amqp091"
	connConfig.Tenant = "/"
	connConfig.PrefetchCount = 5
	return connConfig
}

func produceMessages(conn *grpc.ClientConn, cnt int, message *pb.Message) error {
	c := pb.NewProducerClient(conn)
	ctx := context.Background()

	connConfig := connectConfig()
	defer c.Disconnect(ctx, &pb.Empty{})

	authResp, err := c.Connect(ctx, connConfig)

	if err != nil {
		return err
	}
	if !authResp.GetSuccess() {
		return errors.New(authResp.GetError().GetMessage())
	}

	stream, err := c.Publish(ctx)
	for i := 0; i < cnt; i++ {
		// r, err := c.Publish(ctx, message)
		err = stream.Send(message)
		if err != nil {
			return err
		}
		r, _ := stream.Recv()
		if !r.GetSuccess() {
			return errors.New(r.GetError().GetMessage())
		}
	}
	return nil
}

func consumeMessages(conn *grpc.ClientConn, messages chan<- *pb.Message, done chan bool, clientConnected chan bool, source *pb.Source) error {
	c := pb.NewConsumerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer c.Disconnect(ctx, &pb.Empty{})
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
			return err
		}
		if err != nil {
			break
		}
		messages <- message
		_, err = c.AckMessage(ctx, message)
		if err != nil {
			fmt.Println(err)
			break
		}
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
	address := &pb.Address{Name: "amq.topic", Subject: "sas.test.proxy.TPSCS", Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TPSCS.Consumer", Address: address}
	go consumeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected

	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	err := produceMessages(producerConnection, expectedMessageCount, message)
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
	address := &pb.Address{Name: "amq.topic", Subject: "sas.test.proxy.TPMCM", Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TPMCM.Consumer", Address: address}
	go consumeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected

	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	err := produceMessages(producerConnection, expectedMessageCount, message)
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

	address := &pb.Address{Name: "amq.topic", Subject: "sas.test.proxy.TPFWC", Type: pb.Address_TOPIC}

	stream, err := c.Publish(ctx)
	err = stream.Send(&pb.Message{Body: []byte("message"), Address: address, Persistent: true})
	assert.Nil(t, err)
	r, err := stream.Recv()
	assert.Nil(t, r)
	assert.Contains(t, err.Error(), "Failed to find connection information")
}

func TestAckFailsWithoutConnect(t *testing.T) {
	conn := connect()
	c := pb.NewConsumerClient(conn)
	ctx := context.Background()
	defer c.Disconnect(ctx, &pb.Empty{})
	defer conn.Close()

	address := &pb.Address{Name: "amq.topic", Subject: "sas.test.proxy.TAFWC", Type: pb.Address_TOPIC}

	r, err := c.AckMessage(ctx, &pb.Message{Body: []byte("message"), Address: address, Persistent: true})
	assert.NotNil(t, err)
	assert.Nil(t, r)
	assert.Contains(t, err.Error(), "Failed to find connection information")
}

func TestNackFailsWithoutConnect(t *testing.T) {
	conn := connect()
	c := pb.NewConsumerClient(conn)
	ctx := context.Background()
	defer c.Disconnect(ctx, &pb.Empty{})
	defer conn.Close()

	address := &pb.Address{Name: "amq.topic", Subject: "sas.test.proxy.TAFWC", Type: pb.Address_TOPIC}

	r, err := c.NackMessage(ctx, &pb.Message{Body: []byte("message"), Address: address, Persistent: true})
	assert.NotNil(t, err)
	assert.Nil(t, r)
	assert.Contains(t, err.Error(), "Failed to find connection information")
}

func TestConsumerDisconnectOKWithoutConnect(t *testing.T) {
	conn := connect()
	c := pb.NewConsumerClient(conn)
	ctx := context.Background()
	defer conn.Close()

	r, err := c.Disconnect(ctx, &pb.Empty{})
	assert.Nil(t, err)
	assert.NotNil(t, r)
}

func TestProducerDisconnectOKWithoutConnect(t *testing.T) {
	conn := connect()
	c := pb.NewProducerClient(conn)
	ctx := context.Background()
	defer conn.Close()

	r, err := c.Disconnect(ctx, &pb.Empty{})
	assert.Nil(t, err)
	assert.NotNil(t, r)
}

func TestProduceConsumeFiltersMatchAll(t *testing.T) {
	producerConnection := connect()
	expectedMessageCount := 1
	defer producerConnection.Close()

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)

	consumerConnection := connect()
	defer consumerConnection.Close()
	source := &pb.Source{Name: "sas.test.proxy.TPCFMAll"}
	address := &pb.Address{Name: "sastest.headers", Subject: "sas.test.proxy.TPCFMAll", Type: pb.Address_FILTER}
	filter := &pb.Filter{Type: pb.Filter_ALL}
	matches := make([]*pb.Match, 0)
	matches = append(matches, &pb.Match{Name: "HeaderToMatchAll", Value: "MyFancyValue"})
	matches = append(matches, &pb.Match{Name: "AnotherHeaderToMatchAll", Value: "MyFancyValue"})
	filter.Matches = matches
	source.Filter = filter
	source.Address = address
	go consumeMessages(consumerConnection, messages, done, clientConnected, source)

	<-clientConnected

	headers1 := make(map[string]string)
	headers2 := make(map[string]string)
	headers3 := make(map[string]string)
	headers1["HeaderToMatchAll"] = "MyFancyValue"
	headers2["HeaderToMatchAll"] = "MyNotFancyValue"
	headers3["HeaderToNotMatchAll"] = "MyFancyValue"
	headers4 := make(map[string]string)
	headers4["HeaderToMatchAll"] = "MyFancyValue"        // for consumed message
	headers4["AnotherHeaderToMatchAll"] = "MyFancyValue" // for consumed message

	message := &pb.Message{Body: []byte("mybody1"), Address: address, Headers: headers1}

	err := produceMessages(producerConnection, 1, message)
	assert.Nil(t, err)

	message = &pb.Message{Body: []byte("mybody2"), Address: address, Headers: headers2}
	err = produceMessages(producerConnection, 1, message)
	assert.Nil(t, err)

	message = &pb.Message{Body: []byte("mybody3"), Address: address, Headers: headers3}
	err = produceMessages(producerConnection, 1, message)
	assert.Nil(t, err)

	message = &pb.Message{Body: []byte("mybody4"), Address: address, Headers: headers4}
	err = produceMessages(producerConnection, 1, message) // this message is the one that gets consumed
	assert.Nil(t, err)

	msgCount := 0

	for start := time.Now(); time.Since(start) < 1*time.Second; {
		select {
		case msg := <-messages:
			assert.Equal(t, msg.GetBody(), []byte("mybody4"))
			msgCount++
		case <-done:
			break
		case <-time.After(1 * time.Second):
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestProduceConsumeFiltersMatchAny(t *testing.T) {
	producerConnection := connect()
	expectedMessageCount := 2
	defer producerConnection.Close()

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)

	consumerConnection := connect()
	defer consumerConnection.Close()
	source := &pb.Source{Name: "sas.test.proxy.TPCFMAny"}
	address := &pb.Address{Name: "sastest.headers", Subject: "sas.test.proxy.TPCFMAny", Type: pb.Address_FILTER}
	filter := &pb.Filter{Type: pb.Filter_ANY}
	matches := make([]*pb.Match, 0)
	matches = append(matches, &pb.Match{Name: "HeaderToMatchAny", Value: "MyFancyValue"})
	matches = append(matches, &pb.Match{Name: "OtherHeaderToMatchAny", Value: "AnotherFancyValue"})
	filter.Matches = matches
	source.Filter = filter
	source.Address = address
	go consumeMessages(consumerConnection, messages, done, clientConnected, source)

	<-clientConnected

	headers1 := make(map[string]string)
	headers2 := make(map[string]string)
	headers3 := make(map[string]string)
	headers4 := make(map[string]string)
	headers1["HeaderToMatchAny"] = "MyFancyValue" // should be consumed
	headers2["HeaderToMatchAny"] = "MyNotFancyValue"
	headers3["HeaderToNotMatchAny"] = "MyFancyValue"
	headers4["OtherHeaderToMatchAny"] = "AnotherFancyValue" // should be consumed

	message := &pb.Message{Body: []byte("mybody1"), Address: address, Headers: headers1}

	err := produceMessages(producerConnection, 1, message) // this message gets consumed
	assert.Nil(t, err)

	message = &pb.Message{Body: []byte("mybody2"), Address: address, Headers: headers2}
	err = produceMessages(producerConnection, 1, message)
	assert.Nil(t, err)

	message = &pb.Message{Body: []byte("mybody3"), Address: address, Headers: headers3}
	err = produceMessages(producerConnection, 1, message)
	assert.Nil(t, err)

	message = &pb.Message{Body: []byte("mybody4"), Address: address, Headers: headers4}
	err = produceMessages(producerConnection, 1, message) // this message get consumed
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

func TestProduceSingleConsumeSingleCustomTopicName(t *testing.T) {
	producerConnection := connect()
	expectedMessageCount := 1
	defer producerConnection.Close()

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	// register 2 consumers so we hopeully consume twice
	consumerConnection := connect()
	defer consumerConnection.Close()
	address := &pb.Address{Name: "sastest.topic", Subject: "sas.test.proxy.TPSCSCTN", Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TPSCSCTN.Consumer", Address: address}
	go consumeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected

	done2 := make(chan bool)
	clientConnected2 := make(chan bool)
	consumerConnection2 := connect()
	defer consumerConnection2.Close()
	source.Name = "sas.test.proxy.TPSCSCTN.Consumer2"
	go consumeMessages(consumerConnection2, messages, done2, clientConnected2, source)
	<-clientConnected2

	message := &pb.Message{Body: []byte("myreallycustommessage"), Address: address}

	err := produceMessages(producerConnection, expectedMessageCount, message)
	assert.Nil(t, err)

	msgCount := 0

	messageUUIDs := make([]string, 0)

	for start := time.Now(); time.Since(start) < 1*time.Second; {
		select {
		case msg := <-messages:
			messageUUIDs = append(messageUUIDs, msg.GetUuid())
			msgCount++
		case <-done:
			break
		case <-time.After(1 * time.Second):
			break
		}
	}
	assert.Equal(t, expectedMessageCount*2, msgCount)
	assert.Equal(t, expectedMessageCount*2, len(messageUUIDs))
	assert.NotEqual(t, messageUUIDs[0], messageUUIDs[1])
}

func TestProduceSingleConsumeSingleCustomQueueName(t *testing.T) {
	producerConnection := connect()
	expectedMessageCount := 1
	defer producerConnection.Close()

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	consumerConnection := connect()
	defer consumerConnection.Close()
	address := &pb.Address{Name: "sastest.direct", Subject: "sas.test.proxy.TPSCSCQN", Type: pb.Address_QUEUE}
	source := &pb.Source{Name: "sas.test.proxy.TPSCSCTQN.Consumer", Address: address}
	go consumeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected

	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	err := produceMessages(producerConnection, expectedMessageCount, message)
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

func TestBadSourceOptionsAmqp091(t *testing.T) {
	consumerConnection := connect()
	defer consumerConnection.Close()
	address := &pb.Address{Name: "sastest.topic", Subject: "sas.test.proxy.TSOA", Type: pb.Address_TOPIC}

	opts := make(map[string]string)
	opts["BadOption"] = "10"

	source := &pb.Source{Name: "sas.test.proxy.TSOA.Consumer", Address: address, Options: opts}

	c := pb.NewConsumerClient(consumerConnection)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer c.Disconnect(ctx, &pb.Empty{})
	defer cancel()

	connConfig := connectConfig()

	_, err := c.Connect(ctx, connConfig)
	assert.Nil(t, err)

	stream, err := c.Subscribe(ctx, source)
	var optionError error
	// assert.Equal(t, err.Error(), "BadOption is an unsupported source option")
	for start := time.Now(); time.Since(start) < 1*time.Second; {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			optionError = err
			break
		}
	}

	assert.NotNil(t, optionError)
	assert.Contains(t, optionError.Error(), "provider does not support the following source options: [BadOption]")
}
