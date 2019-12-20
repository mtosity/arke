package integration

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	pb "sassoftware.io/convoy/arke/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
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

	providerTLS := strings.ToLower(os.Getenv("PROVIDER_TLS"))

	if providerTLS == "sendca" {
		cacert, err := ioutil.ReadFile("certs/testca/ca_certificate.pem")
		if err != nil {
			log.Fatalf("Error reading provider CA cert: %v", err)
		}
		connConfig.Tls = true
		connConfig.CaCertificate = cacert
		connConfig.Port = 5671
	} else if providerTLS == "true" {
		connConfig.Tls = true
		connConfig.Port = 5671
	}

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
		err = stream.Send(message)
		if err != nil {
			fmt.Println(err)
			return err
		}
		r, _ := stream.Recv()
		if !r.GetSuccess() {
			return errors.New(r.GetError().GetMessage())
		}
	}
	return nil
}

func subscribeMessages(conn *grpc.ClientConn, messages chan<- *pb.Message, done chan bool, clientConnected chan bool, source *pb.Source) error {
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

	stream, err := c.Consume(ctx)

	if err != nil {
		log.Panicf("could not subscribe: %v", err)
	}

	m := &pb.Consume{}
	m.Msg = &pb.Consume_Src{Src: source}
	stream.SendMsg(m)
	defer stream.CloseSend()

	stop := false

	for start := time.Now(); time.Since(start) < 2*time.Second; {
		resp, _ := stream.Recv()
		if resp.GetMsg() != nil {
			go func(stop *bool) {
				if *stop {
					return
				}
				message := resp.GetMsg()
				if err == io.EOF {
					log.Panicf("error: %s", err.Error())
					return
				}
				if err != nil {
					if message != nil {
						if message.GetError().GetIsFatal() {
							log.Printf("Fatal error 1: %v.Subscribe(_) = _, %v", c, err)
							return
						}
						log.Printf("Error: %v", err)
						return
					}
					log.Printf("Fatal error 2: %v.Subscribe(_) = _, %v", c, err)
					return
				}

				ret := &pb.Consume{Msg: &pb.Consume_Ack{Ack: &pb.MessageConsumed{Uuid: message.GetUuid()}}}
				err = stream.Send(ret)

				if err != nil {
					log.Println(err)
					return
				}

				messages <- message
			}(&stop)

		} else if resp.GetConsumedResponse() != nil {
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

	var conn *grpc.ClientConn
	var err error
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

	// Attempt a non-TLS connection to arke first
	conn, err = grpc.Dial(arkeAddress, grpc.WithInsecure())

	c := healthpb.NewHealthClient(conn)
	resp, err := c.Check(ctx, &healthpb.HealthCheckRequest{Service: "arke"})

	// If the health check failed, try with TLS
	if err != nil && (resp == nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING) {
		b, err := ioutil.ReadFile("certs/testca/ca_certificate.pem")
		if err != nil {
			log.Fatal(err)
		}
		cp := x509.NewCertPool()
		if !cp.AppendCertsFromPEM(b) {
			log.Fatalf("client did not connect: %v", "credentials: failed to append certificates")
		}
		tlsConfig := &tls.Config{RootCAs: cp}

		conn, err = grpc.Dial(arkeAddress, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))) // , grpc.WithInsecure()

		c := healthpb.NewHealthClient(conn)
		resp, err = c.Check(ctx, &healthpb.HealthCheckRequest{Service: "arke"})
	}

	if err != nil && resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
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
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPSCS")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TPSCS.Consumer", Address: address, PrefetchCount: 5}
	go consumeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected

	time.Sleep(200 * time.Millisecond)

	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	err := produceMessages(producerConnection, expectedMessageCount, message)
	assert.Nil(t, err)

	msgCount := 0

	for start := time.Now(); time.Since(start) < 2*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			break
		case <-time.After(2 * time.Second):
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestProduceSingleSubscribeSingle(t *testing.T) {
	producerConnection := connect()
	expectedMessageCount := 1
	defer producerConnection.Close()

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	consumerConnection := connect()
	defer consumerConnection.Close()
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPSCS")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TPSCS.Consumer", Address: address, PrefetchCount: 5}
	go subscribeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected

	time.Sleep(500 * time.Millisecond)

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
		case <-time.After(2 * time.Second):
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestProduceManySubscribeMany(t *testing.T) {
	producerConnection := connect()
	expectedMessageCount := 30
	defer producerConnection.Close()

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)

	consumerConnection := connect()
	defer consumerConnection.Close()
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPMCM")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TPMCM.Consumer", Address: address, PrefetchCount: 5}
	go subscribeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected

	time.Sleep(1000 * time.Millisecond)

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
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPMCM")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TPMCM.Consumer", Address: address, PrefetchCount: 5}
	go consumeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected

	time.Sleep(1000 * time.Millisecond)

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

	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPFWC")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}

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

	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TAFWC")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}

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

	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TAFWC")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}

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

func TestProduceSubscribeFiltersMatchAll(t *testing.T) {
	producerConnection := connect()
	expectedMessageCount := 1
	defer producerConnection.Close()

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)

	consumerConnection := connect()
	defer consumerConnection.Close()
	source := &pb.Source{Name: "sas.test.proxy.TPCFMAll", PrefetchCount: 5}
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPCFMAll")
	address := &pb.Address{Name: "sastest.headers", Subjects: subjects, Type: pb.Address_FILTER}
	filter := &pb.Filter{Type: pb.Filter_ALL}
	matches := make([]*pb.Match, 0)
	matches = append(matches, &pb.Match{Name: "HeaderToMatchAll", Value: "MyFancyValue"})
	matches = append(matches, &pb.Match{Name: "AnotherHeaderToMatchAll", Value: "MyFancyValue"})
	filter.Matches = matches
	source.Filters = make([]*pb.Filter, 0)
	source.Filters = append(source.Filters, filter)
	source.Address = address
	go subscribeMessages(consumerConnection, messages, done, clientConnected, source)

	<-clientConnected
	time.Sleep(500 * time.Millisecond)

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

func TestProduceConsumeFiltersMatchAll(t *testing.T) {
	producerConnection := connect()
	expectedMessageCount := 1
	defer producerConnection.Close()

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)

	consumerConnection := connect()
	defer consumerConnection.Close()
	source := &pb.Source{Name: "sas.test.proxy.TPCFMAll", PrefetchCount: 5}
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPCFMAll")
	address := &pb.Address{Name: "sastest.headers", Subjects: subjects, Type: pb.Address_FILTER}
	filter := &pb.Filter{Type: pb.Filter_ALL}
	matches := make([]*pb.Match, 0)
	matches = append(matches, &pb.Match{Name: "HeaderToMatchAll", Value: "MyFancyValue"})
	matches = append(matches, &pb.Match{Name: "AnotherHeaderToMatchAll", Value: "MyFancyValue"})
	filter.Matches = matches
	source.Filters = make([]*pb.Filter, 0)
	source.Filters = append(source.Filters, filter)
	source.Address = address
	go consumeMessages(consumerConnection, messages, done, clientConnected, source)

	<-clientConnected
	time.Sleep(500 * time.Millisecond)

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

func TestProduceSubscribeFiltersMatchAny(t *testing.T) {
	producerConnection := connect()
	expectedMessageCount := 2
	defer producerConnection.Close()

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)

	consumerConnection := connect()
	defer consumerConnection.Close()
	source := &pb.Source{Name: "sas.test.proxy.TPCFMAny", PrefetchCount: 5}
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPCFMAny")
	address := &pb.Address{Name: "sastest.headers", Subjects: subjects, Type: pb.Address_FILTER}
	filter := &pb.Filter{Type: pb.Filter_ANY}
	matches := make([]*pb.Match, 0)
	matches = append(matches, &pb.Match{Name: "HeaderToMatchAny", Value: "MyFancyValue"})
	matches = append(matches, &pb.Match{Name: "OtherHeaderToMatchAny", Value: "AnotherFancyValue"})
	filter.Matches = matches
	source.Filters = make([]*pb.Filter, 0)
	source.Filters = append(source.Filters, filter)
	source.Address = address
	go subscribeMessages(consumerConnection, messages, done, clientConnected, source)

	<-clientConnected

	time.Sleep(500 * time.Millisecond)

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

func TestProduceConsumeFiltersMatchAny(t *testing.T) {
	producerConnection := connect()
	expectedMessageCount := 2
	defer producerConnection.Close()

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)

	consumerConnection := connect()
	defer consumerConnection.Close()
	source := &pb.Source{Name: "sas.test.proxy.TPCFMAny", PrefetchCount: 5}
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPCFMAny")
	address := &pb.Address{Name: "sastest.headers", Subjects: subjects, Type: pb.Address_FILTER}
	filter := &pb.Filter{Type: pb.Filter_ANY}
	matches := make([]*pb.Match, 0)
	matches = append(matches, &pb.Match{Name: "HeaderToMatchAny", Value: "MyFancyValue"})
	matches = append(matches, &pb.Match{Name: "OtherHeaderToMatchAny", Value: "AnotherFancyValue"})
	filter.Matches = matches
	source.Filters = make([]*pb.Filter, 0)
	source.Filters = append(source.Filters, filter)
	source.Address = address
	go consumeMessages(consumerConnection, messages, done, clientConnected, source)

	<-clientConnected

	time.Sleep(500 * time.Millisecond)

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

func TestProduceSingleSubscribeSingleCustomTopicName(t *testing.T) {
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
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPSCSCTN")
	address := &pb.Address{Name: "sastest.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TPSCSCTN.Consumer", Address: address, PrefetchCount: 5}
	go subscribeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected

	done2 := make(chan bool)
	clientConnected2 := make(chan bool)
	consumerConnection2 := connect()
	defer consumerConnection2.Close()
	source.Name = "sas.test.proxy.TPSCSCTN.Consumer2"
	go subscribeMessages(consumerConnection2, messages, done2, clientConnected2, source)
	<-clientConnected2

	time.Sleep(500 * time.Millisecond)

	message := &pb.Message{Body: []byte("myreallycustommessage"), Address: address}

	err := produceMessages(producerConnection, expectedMessageCount, message)
	assert.Nil(t, err)

	msgCount := 0

	messageUUIDs := make([]string, 0)

	for start := time.Now(); time.Since(start) < 2*time.Second; {
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
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPSCSCTN")
	address := &pb.Address{Name: "sastest.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TPSCSCTN.Consumer", Address: address, PrefetchCount: 5}
	go consumeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected

	done2 := make(chan bool)
	clientConnected2 := make(chan bool)
	consumerConnection2 := connect()
	defer consumerConnection2.Close()
	source.Name = "sas.test.proxy.TPSCSCTN.Consumer2"
	go consumeMessages(consumerConnection2, messages, done2, clientConnected2, source)
	<-clientConnected2

	time.Sleep(500 * time.Millisecond)

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

func TestProduceSingleSubscribeSingleCustomQueueName(t *testing.T) {
	producerConnection := connect()
	expectedMessageCount := 1
	defer producerConnection.Close()

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	consumerConnection := connect()
	defer consumerConnection.Close()
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPSCSCQN")
	address := &pb.Address{Name: "sastest.direct", Subjects: subjects, Type: pb.Address_QUEUE}
	source := &pb.Source{Name: "sas.test.proxy.TPSCSCTQN.Consumer", Address: address, PrefetchCount: 5}
	go subscribeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected

	time.Sleep(500 * time.Millisecond)

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
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPSCSCQN")
	address := &pb.Address{Name: "sastest.direct", Subjects: subjects, Type: pb.Address_QUEUE}
	source := &pb.Source{Name: "sas.test.proxy.TPSCSCTQN.Consumer", Address: address, PrefetchCount: 5}
	go consumeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected

	time.Sleep(500 * time.Millisecond)

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

func TestHeaders_Subscribe(t *testing.T) {
	producerConnection := connect()
	expectedMessageCount := 1
	defer producerConnection.Close()

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	consumerConnection := connect()
	defer consumerConnection.Close()
	headers := make(map[string]string)
	headers["header-one"] = "one"
	headers["content-type"] = "text/json"
	headers["Content-Type"] = "text/yaml"
	headers["CONTENT-ENCODING"] = "base64"
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TH")
	address := &pb.Address{Name: "sastest.direct", Subjects: subjects, Type: pb.Address_QUEUE}
	source := &pb.Source{Name: "sas.test.proxy.TH.Consumer", Address: address, PrefetchCount: 5}
	go subscribeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected

	time.Sleep(250 * time.Millisecond)

	message := &pb.Message{Body: []byte("mymessage"), Address: address, Headers: headers}

	err := produceMessages(producerConnection, expectedMessageCount, message)
	assert.Nil(t, err)

	msgCount := 0

	received := make([]*pb.Message, 0)
	for start := time.Now(); time.Since(start) < 1*time.Second; {
		select {
		case msg := <-messages:
			received = append(received, msg)
			msgCount++
		case <-done:
			break
		case <-time.After(1 * time.Second):
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
	assert.Equal(t, headers, received[0].Headers)
	assert.NotNil(t, received[0].GetAddress())
}

func TestHeaders_Consume(t *testing.T) {
	producerConnection := connect()
	expectedMessageCount := 1
	defer producerConnection.Close()

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	consumerConnection := connect()
	defer consumerConnection.Close()
	headers := make(map[string]string)
	headers["header-one"] = "one"
	headers["content-type"] = "text/json"
	headers["Content-Type"] = "text/yaml"
	headers["CONTENT-ENCODING"] = "base64"
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TH")
	address := &pb.Address{Name: "sastest.direct", Subjects: subjects, Type: pb.Address_QUEUE}
	source := &pb.Source{Name: "sas.test.proxy.TH.Consumer", Address: address, PrefetchCount: 5}
	go consumeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected

	time.Sleep(250 * time.Millisecond)

	message := &pb.Message{Body: []byte("mymessage"), Address: address, Headers: headers}

	err := produceMessages(producerConnection, expectedMessageCount, message)
	assert.Nil(t, err)

	msgCount := 0

	received := make([]*pb.Message, 0)
	for start := time.Now(); time.Since(start) < 1*time.Second; {
		select {
		case msg := <-messages:
			received = append(received, msg)
			msgCount++
		case <-done:
			break
		case <-time.After(1 * time.Second):
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
	assert.Equal(t, headers, received[0].Headers)
	assert.NotNil(t, received[0].GetAddress())
}
func TestProduceManySubscribeManyExclusive(t *testing.T) {
	producerConnection := connect()
	expectedMessageCount := 1
	defer producerConnection.Close()

	messages := make(chan *pb.Message)
	messages2 := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	// register 2 consumers so we hopeully consume twice
	consumerConnection := connect()
	defer consumerConnection.Close()
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPMCME")
	address := &pb.Address{Name: "sastest.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TPMCME.Consumer", Address: address, PrefetchCount: 5}
	go subscribeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected

	done2 := make(chan bool)
	clientConnected2 := make(chan bool)
	consumerConnection2 := connect()
	defer consumerConnection2.Close()
	source.Name = "sas.test.proxy.TPMCME.Consumer"
	go subscribeMessages(consumerConnection2, messages2, done2, clientConnected2, source)
	<-clientConnected2

	message := &pb.Message{Body: []byte("myreallycustommessage"), Address: address}

	err := produceMessages(producerConnection, expectedMessageCount, message)
	assert.Nil(t, err)

	msgCount := 0
	msgCount2 := 0

	messageUUIDs := make([]string, 0)
	messageUUIDs2 := make([]string, 0)

	for start := time.Now(); time.Since(start) < 1*time.Second; {
		select {
		case msg := <-messages:
			messageUUIDs = append(messageUUIDs, msg.GetUuid())
			msgCount++
		case msg := <-messages2:
			messageUUIDs2 = append(messageUUIDs2, msg.GetUuid())
			msgCount2++
		case <-done:
			break
		case <-time.After(1 * time.Second):
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
	assert.Equal(t, expectedMessageCount, len(messageUUIDs))
	assert.Equal(t, 0, msgCount2)
	assert.Equal(t, 0, len(messageUUIDs2))
}

func TestProduceManyConsumeManyExclusive(t *testing.T) {
	producerConnection := connect()
	expectedMessageCount := 1
	defer producerConnection.Close()

	messages := make(chan *pb.Message)
	messages2 := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	// register 2 consumers so we hopeully consume twice
	consumerConnection := connect()
	defer consumerConnection.Close()
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPMCME")
	address := &pb.Address{Name: "sastest.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TPMCME.Consumer", Address: address, PrefetchCount: 5}
	go consumeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected

	done2 := make(chan bool)
	clientConnected2 := make(chan bool)
	consumerConnection2 := connect()
	defer consumerConnection2.Close()
	source.Name = "sas.test.proxy.TPMCME.Consumer"
	go consumeMessages(consumerConnection2, messages2, done2, clientConnected2, source)
	<-clientConnected2

	message := &pb.Message{Body: []byte("myreallycustommessage"), Address: address}

	err := produceMessages(producerConnection, expectedMessageCount, message)
	assert.Nil(t, err)

	msgCount := 0
	msgCount2 := 0

	messageUUIDs := make([]string, 0)
	messageUUIDs2 := make([]string, 0)

	for start := time.Now(); time.Since(start) < 1*time.Second; {
		select {
		case msg := <-messages:
			messageUUIDs = append(messageUUIDs, msg.GetUuid())
			msgCount++
		case msg := <-messages2:
			messageUUIDs2 = append(messageUUIDs2, msg.GetUuid())
			msgCount2++
		case <-done:
			break
		case <-time.After(1 * time.Second):
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
	assert.Equal(t, expectedMessageCount, len(messageUUIDs))
	assert.Equal(t, 0, msgCount2)
	assert.Equal(t, 0, len(messageUUIDs2))
}
func TestSubscribeMultiSubject(t *testing.T) {
	producerConnection := connect()
	produceCount := 5
	defer producerConnection.Close()

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)

	consumerConnection := connect()
	defer consumerConnection.Close()

	// Subscribe with 2 subject bindings
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TSMS.1")
	subjects = append(subjects, "sas.test.proxy.TSMS.2")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TSMS.Consumer", Address: address, PrefetchCount: 5}
	go subscribeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected
	time.Sleep(500 * time.Millisecond)

	// Produce to binding one
	subjects = make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TSMS.1")
	address.Subjects = subjects
	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	err := produceMessages(producerConnection, produceCount, message)
	assert.Nil(t, err)

	// Produce to binding 2
	subjects = make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TSMS.2")
	address.Subjects = subjects
	message = &pb.Message{Body: []byte("mymessage"), Address: address}

	err = produceMessages(producerConnection, produceCount, message)
	assert.Nil(t, err)

	msgCount := 0

	for start := time.Now(); time.Since(start) < 2*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			break
		case <-time.After(2 * time.Second):
			break
		}
	}
	assert.Equal(t, 2*produceCount, msgCount)
}

func TestConsumeMultiSubject(t *testing.T) {
	producerConnection := connect()
	produceCount := 5
	defer producerConnection.Close()

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)

	consumerConnection := connect()
	defer consumerConnection.Close()

	// Subscribe with 2 subject bindings
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TSMS.1")
	subjects = append(subjects, "sas.test.proxy.TSMS.2")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TSMS.Consumer", Address: address, PrefetchCount: 5}
	go consumeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected
	time.Sleep(500 * time.Millisecond)

	// Produce to binding one
	subjects = make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TSMS.1")
	address.Subjects = subjects
	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	err := produceMessages(producerConnection, produceCount, message)
	assert.Nil(t, err)

	// Produce to binding 2
	subjects = make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TSMS.2")
	address.Subjects = subjects
	message = &pb.Message{Body: []byte("mymessage"), Address: address}

	err = produceMessages(producerConnection, produceCount, message)
	assert.Nil(t, err)

	msgCount := 0

	for start := time.Now(); time.Since(start) < 2*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			break
		case <-time.After(2 * time.Second):
			break
		}
	}
	assert.Equal(t, 2*produceCount, msgCount)
}
func TestProduceMultiSubject_FAIL(t *testing.T) {
	producerConnection := connect()
	produceCount := 5
	defer producerConnection.Close()

	// Publish with 2 subject bindings
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TSMS.1")
	subjects = append(subjects, "sas.test.proxy.TSMS.2")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	err := produceMessages(producerConnection, produceCount, message)
	assert.NotNil(t, err)

	assert.Contains(t, err.Error(), "exactly one subject allowed in an Address")
}

func TestParentExchange_Subscribe(t *testing.T) {
	// Create a ParentAddress with name test.parent
	// Create an Address with name test.child and Parent ParentAddress
	// Consume messages from queue bound to test.child
	// Produce messages to test.parent

	producerConnection := connect()
	produceCount := 5
	defer producerConnection.Close()

	subjects := make([]string, 0)
	parentSubjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPE.1")
	subjects = append(subjects, "sas.test.proxy.TPE.2")
	parentSubjects = append(parentSubjects, "sas.test.proxy.TPE.1")

	parent := &pb.Address{Name: "test.parent", Type: pb.Address_TOPIC, Subjects: parentSubjects}

	child := &pb.Address{Name: "test.child", Subjects: subjects, Type: pb.Address_FILTER, ParentAddress: parent}

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)

	consumerConnection := connect()
	defer consumerConnection.Close()

	// Subscribe to the child Address
	source := &pb.Source{Name: "sas.test.proxy.TPE.Consumer", Address: child, PrefetchCount: 5}
	go subscribeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected

	time.Sleep(500 * time.Millisecond)

	// Publish to the parent address
	message := &pb.Message{Body: []byte("mymessage"), Address: parent}
	err := produceMessages(producerConnection, produceCount, message)
	assert.Nil(t, err)

	msgCount := 0

	for start := time.Now(); time.Since(start) < 1*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			break
		case <-time.After(2 * time.Second):
			break
		}
	}
	assert.Equal(t, produceCount, msgCount)
}

func TestParentExchange_Consume(t *testing.T) {
	// Create a ParentAddress with name test.parent
	// Create an Address with name test.child and Parent ParentAddress
	// Consume messages from queue bound to test.child
	// Produce messages to test.parent

	producerConnection := connect()
	produceCount := 5
	defer producerConnection.Close()

	subjects := make([]string, 0)
	parentSubjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPE.1")
	subjects = append(subjects, "sas.test.proxy.TPE.2")
	parentSubjects = append(parentSubjects, "sas.test.proxy.TPE.1")

	parent := &pb.Address{Name: "test.parent", Type: pb.Address_TOPIC, Subjects: parentSubjects}

	child := &pb.Address{Name: "test.child", Subjects: subjects, Type: pb.Address_FILTER, ParentAddress: parent}

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)

	consumerConnection := connect()
	defer consumerConnection.Close()

	// Subscribe to the child Address
	source := &pb.Source{Name: "sas.test.proxy.TPE.Consumer", Address: child, PrefetchCount: 5}
	go consumeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected

	time.Sleep(500 * time.Millisecond)

	// Publish to the parent address
	message := &pb.Message{Body: []byte("mymessage"), Address: parent}
	err := produceMessages(producerConnection, produceCount, message)
	assert.Nil(t, err)

	msgCount := 0

	for start := time.Now(); time.Since(start) < 1*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			break
		case <-time.After(2 * time.Second):
			break
		}
	}
	assert.Equal(t, produceCount, msgCount)
}
func TestAddressType_FAIL(t *testing.T) {
	// Create a ParentAddress with name test.parent
	// Create an Address with name test.child and Parent ParentAddress
	// Consume messages from queue bound to test.child
	// Produce messages to test.parent

	producerConnection := connect()
	produceCount := 5
	defer producerConnection.Close()

	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TATF.1")

	parent := &pb.Address{Name: "test.parent", Type: 5, Subjects: subjects}

	message := &pb.Message{Body: []byte("mymessage"), Address: parent}
	err := produceMessages(producerConnection, produceCount, message)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "5 is not a valid address type")
}

func TestHeadersNoSubscribeSubject(t *testing.T) {
	producerConnection := connect()
	expectedMessageCount := 30
	defer producerConnection.Close()

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)

	consumerConnection := connect()
	defer consumerConnection.Close()
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.THNSS")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.THNSS.Consumer", Address: address, PrefetchCount: 5}
	go subscribeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected

	time.Sleep(500 * time.Millisecond)

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

func TestHeadersNoConsumeSubject(t *testing.T) {
	producerConnection := connect()
	expectedMessageCount := 30
	defer producerConnection.Close()

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)

	consumerConnection := connect()
	defer consumerConnection.Close()
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.THNSS")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.THNSS.Consumer", Address: address, PrefetchCount: 5}
	go consumeMessages(consumerConnection, messages, done, clientConnected, source)
	<-clientConnected

	time.Sleep(500 * time.Millisecond)

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
