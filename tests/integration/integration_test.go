//go:build integration
// +build integration

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
	"strconv"
	"strings"
	"testing"
	"time"

	azadmin "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"gopkg.in/yaml.v2"
	pb "sassoftware.io/viya/arke/api"
	cfg "sassoftware.io/viya/arke/test/config"
)

type ComposeFile struct {
	Services map[string]struct {
		Environment []string `yaml:"environment"`
	} `yaml:"services"`
}

type RateLimitSettings struct {
	Enforced          bool
	BucketSize        int
	RefillSeconds     time.Duration
	MaxAgeStaleClents time.Duration
}

func readCompose(composeFile string) (*ComposeFile, error) {

	data, err := os.ReadFile(composeFile)
	if err != nil {
		return nil, err
	}

	var compose ComposeFile
	err = yaml.Unmarshal(data, &compose)
	if err != nil {
		return nil, err
	}
	return &compose, nil
}

func GetEnvMapFromList(nameEqualValue []string) map[string]string {
	m := make(map[string]string)
	for _, nvp := range nameEqualValue {
		if !strings.Contains(nvp, "=") {
			continue
		}
		parts := strings.SplitN(nvp, "=", 2)
		m[parts[0]] = parts[1]
	}
	return m
}

func GetMapBool(vars map[string]string, varname string) (val bool, ok bool, err error) {
	sval, ok := vars[varname]
	if !ok {
		return false, false, fmt.Errorf("value not set: %s", varname)
	}
	val, err = strconv.ParseBool(sval)
	if err != nil {
		return false, true, fmt.Errorf("invalid bool value: %s", sval)
	}
	return val, true, nil
}

func GetMapInt(vars map[string]string, varname string) (val int, ok bool, err error) {
	sval, ok := vars[varname]
	if !ok {
		return 0, false, fmt.Errorf("value not set: %s", varname)
	}
	val, err = strconv.Atoi(sval)
	if err != nil {
		return 0, true, err
	}
	return val, true, nil
}

// GetEnvMap returns a map of env vars from either the docker-compose.yml file if present
// or os.Environ()
func GetEnvMap(t *testing.T, composeFile string) map[string]string {
	var envVars map[string]string
	compose, err := readCompose(composeFile)
	if err == nil {
		t.Logf("Getting environment variables from %s", composeFile)
		arkeCompose, ok := compose.Services["arke"]
		assert.True(t, ok, "could not find arke service in %s", composeFile)
		envVars = GetEnvMapFromList(arkeCompose.Environment)
	} else {
		t.Logf("Error reading from docker-compose.yml -- probably in viya-oci-arke: %v", err)
		envVars = GetEnvMapFromList(os.Environ())
	}
	return envVars
}

func GetRateLimitValues(t *testing.T, composeFile string) (*RateLimitSettings, error) {
	vars := GetEnvMap(t, composeFile)
	enforced, isSet, err := GetMapBool(vars, "RATE_LIMIT_ENFORCED")
	if !isSet {
		return nil, fmt.Errorf("rate limit enforcement not set")
	}
	if err != nil {
		return nil, err
	}

	bktSize, isSet, err := GetMapInt(vars, "RATE_LIMIT_BUCKET_SIZE")
	if !isSet {
		return nil, fmt.Errorf("rate limit bucket size not set")
	}
	if err != nil {
		return nil, err
	}
	refill, isSet, err := GetMapInt(vars, "RATE_LIMIT_REFILL_SECONDS")
	if !isSet {
		return nil, fmt.Errorf("rate limit refill seconds not set")
	}
	if err != nil {
		return nil, err
	}
	maxAge, isSet, err := GetMapInt(vars, "RATE_LIMIT_MAX_AGE_STALE_CLIENTS")
	if !isSet {
		return nil, fmt.Errorf("rate limit max age stale clients not set")
	}
	if err != nil {
		return nil, err
	}

	return &RateLimitSettings{
		Enforced:          enforced,
		BucketSize:        bktSize,
		RefillSeconds:     time.Duration(refill) * time.Second,
		MaxAgeStaleClents: time.Duration(maxAge) * time.Second,
	}, nil
}

func cleanupAzure() {
	if providerType, ok := os.LookupEnv("SAS_BROKER_TYPE"); ok {
		if providerType == "azure" {
			connConfig := cfg.ConnectionConfigurationFromEnv()
			connStr := fmt.Sprintf("Endpoint=sb://%s/;SharedAccessKeyName=%s;SharedAccessKey=%s",
				connConfig.Host, connConfig.GetCredentials().GetUsername(),
				connConfig.GetCredentials().GetPassword())
			adminClient, err := azadmin.NewClientFromConnectionString(connStr, nil)
			if err != nil {
				panic("could not to connect to azure for cleanup")
			}
			ctx := context.Background()
			pager := adminClient.NewListTopicsPager(nil)
			for pager.More() {
				page, err := pager.NextPage(ctx)
				if err != nil {
					fmt.Println("Error listing topics:", err)
					return
				}
				for _, topic := range page.Topics {
					_, err := adminClient.DeleteTopic(ctx, topic.TopicName, nil)
					if err != nil {
						fmt.Printf("Error deleting topic %s: %s", topic.TopicName, err)
					}
				}
			}
		}
	}
}

type MsgHandler func(msg *pb.Message) (int, error)

func defaultHandler(msg *pb.Message) (int, error) {
	return 0, nil
}

func arkeAddress() string {
	var arkeAddress string
	var arkeHost string
	var arkePort string
	if value, ok := os.LookupEnv("ARKE_INTEGRATION_HOSTNAME"); ok {
		arkeHost = value
	} else {
		arkeHost = "localhost"
	}
	if value, ok := os.LookupEnv("ARKE_INTEGRATION_PORT"); ok {
		arkePort = value
	} else {
		arkePort = "50051"
	}

	arkeAddress = fmt.Sprintf("%s:%s", arkeHost, arkePort)
	return arkeAddress
}

func connectConfig() *pb.ConnectionConfiguration {

	connConfig := cfg.ConnectionConfigurationFromEnv()

	providerTLS := strings.ToLower(os.Getenv("PROVIDER_TLS"))

	if providerTLS == "sendca" {
		cacert, err := ioutil.ReadFile("certs/testca/ca_certificate.pem")
		if err != nil {
			log.Fatalf("Error reading provider CA cert: %v", err)
		}
		connConfig.Tls = true
		connConfig.CaCertificate = cacert
	} else if providerTLS == "true" {
		connConfig.Tls = true
	}

	return &connConfig
}

func produceMessages(conn *grpc.ClientConn, c pb.ProducerClient, ctx context.Context, cnt int, message *pb.Message) error { //nolint

	connConfig := connectConfig()
	defer c.Disconnect(ctx, &pb.Empty{})

	authResp, err := c.Connect(ctx, connConfig)

	if err != nil {
		return err
	}
	if !authResp.GetSuccess() {
		return errors.New(authResp.GetError().GetMessage())
	}

	stream, _ := c.Publish(ctx)
	for i := 0; i < cnt; i++ {
		err = stream.Send(message)
		if err != nil {
			fmt.Println(err)
			return err
		}
		r, err := stream.Recv()
		if err != nil && err == io.EOF {
			return nil
		}
		if r != nil && !r.GetSuccess() {
			return errors.New(r.GetError().GetMessage())
		}
	}
	return nil
}

func produceMessagesUnary(conn *grpc.ClientConn, c pb.ProducerClient, ctx context.Context, cnt int, message *pb.Message) error { //nolint

	connConfig := connectConfig()
	defer c.Disconnect(ctx, &pb.Empty{})

	authResp, err := c.Connect(ctx, connConfig)

	if err != nil {
		return err
	}
	if !authResp.GetSuccess() {
		return errors.New(authResp.GetError().GetMessage())
	}

	for i := 0; i < cnt; i++ {
		resp, err := c.PublishOne(ctx, message)
		if err != nil {
			fmt.Println(err)
			return err
		}
		if resp != nil && !resp.GetSuccess() {
			return errors.New(resp.GetError().GetMessage())
		}
	}
	return nil
}

// TODO: pass in a message handler to control ack/nack
func consumeMessages(conn *grpc.ClientConn, c pb.ConsumerClient, ctx context.Context, messages chan<- *pb.Message, done chan bool, clientConnected chan bool, source *pb.Source, handler MsgHandler, t *testing.T) error { //nolint

	defer c.Disconnect(ctx, &pb.Empty{})

	connConfig := connectConfig()

	authResp, err := c.Connect(ctx, connConfig)

	if err != nil {
		log.Panicf("could not authenticate: %v", err)
	}
	if !authResp.GetSuccess() {
		log.Panicf("could not authenticate: %v", authResp.GetError().GetMessage())
	}

	stream, err := c.Consume(ctx)

	if err != nil {
		log.Panicf("could not subscribe: %v", err)
	}

	m := &pb.Consume{}
	m.Msg = &pb.Consume_Src{Src: source}
	stream.SendMsg(m)
	defer stream.CloseSend()

	if providerType, ok := os.LookupEnv("SAS_BROKER_TYPE"); ok {
		var sleep time.Duration
		switch providerType {
		case "azure":
			sleep = 4000
		default:
			sleep = 500
		}
		time.Sleep(sleep * time.Millisecond)
	}

	clientConnected <- true

	stop := false

	for start := time.Now(); time.Since(start) < 30*time.Second; {
		resp, _ := stream.Recv()
		if resp.GetMsg() != nil {
			go func(stop *bool) {
				if *stop {
					return
				}
				message := resp.GetMsg()
				// TODO: err is not used inside this for loop except down
				// below and it returns. I think it is safe to remove this code.
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

				delay, ackErr := handler(message)
				nack := true
				if ackErr == nil {
					nack = false
				}
				ret := &pb.Consume{Msg: &pb.Consume_Ack{Ack: &pb.MessageConsumed{Nack: nack, RequeueDelay: int32(delay), Uuid: message.GetUuid()}}}
				err = stream.Send(ret)

				if err != nil {
					log.Println(err)
					return
				}

				messages <- message
			}(&stop)

		} else if resp.GetConsumedResponse() != nil {
			assert.NotEmpty(t, resp.GetConsumedResponse().GetUuid())
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Attempt a non-TLS connection to arke first
	conn, _ = grpc.NewClient(arkeAddress(), grpc.WithInsecure())

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

		conn, _ = grpc.NewClient(arkeAddress(), grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))) // , grpc.WithInsecure()

		c := healthpb.NewHealthClient(conn)
		resp, err = c.Check(ctx, &healthpb.HealthCheckRequest{Service: "arke"})
	}

	if err != nil && resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		log.Fatalf("client did not connect: %v", err)
	}
	return conn
}

func Test_CleanupAzureNamespace_NotActuallyATest(t *testing.T) {
	if _, ok := os.LookupEnv("SKIP_CLEANUP"); !ok {
		cleanupAzure()
		assert.True(t, true)
	}
}

func TestProduceSingleConsumeSingle(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 1
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	consumerConnection := connect()
	defer consumerConnection.Close()
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPSCS")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TPSCS.Consumer", Address: address, PrefetchCount: 1}
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	err := produceMessages(producerConnection, pc, pctx, expectedMessageCount, message)
	assert.Nil(t, err)

	msgCount := 0

	breakLoop := false
	for start := time.Now(); time.Since(start) < 2*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(2 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestProduceOneConsumeOne(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 1
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	consumerConnection := connect()
	defer consumerConnection.Close()
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.PubOne")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.PubOne.Consumer", Address: address, PrefetchCount: 1}
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	err := produceMessagesUnary(producerConnection, pc, pctx, expectedMessageCount, message)
	assert.Nil(t, err)

	msgCount := 0

	breakLoop := false
	for start := time.Now(); time.Since(start) < 2*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(2 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestProduceOneRepeatedConsumeOne(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 50
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	consumerConnection := connect()
	defer consumerConnection.Close()
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.PubOne")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.PubOne.Consumer", Address: address, PrefetchCount: 1}
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	err := produceMessagesUnary(producerConnection, pc, pctx, expectedMessageCount, message)
	assert.Nil(t, err)

	msgCount := 0

	breakLoop := false
	for start := time.Now(); time.Since(start) < 2*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(2 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestProduceOneFailsWithoutConnect(t *testing.T) {
	conn := connect()
	defer conn.Close()
	c := pb.NewProducerClient(conn)
	ctx := context.Background()
	defer c.Disconnect(ctx, &pb.Empty{})

	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPFWC")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}

	msg := &pb.Message{Body: []byte("message"), Address: address, Persistent: true}
	_, err := c.PublishOne(ctx, msg)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Could not find client identifier")

	// TODO: Why is the MessageResponse nil?
	//assert.NotNil(t, resp)
	//assert.False(t, resp.GetSuccess())
	//assert.Contains(t, resp.GetError(), "Could not find client identifier")
}

func TestProduceSingleConsumeRetry(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 6
	produceMessageCount := 1
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)

	//retry handler
	retryHandler := func(msg *pb.Message) (int, error) {
		headers := msg.GetHeaders()
		count := 0
		delay := 0
		var err error

		if xDeath, ok := headers["x-death"]; ok {
			pieces := strings.Split(xDeath, " ")
			aCount := strings.Split(pieces[0], ":")[1]
			count, _ = strconv.Atoi(aCount)
		}

		if count < 5 {
			delay = 2
			err = errors.New("Attempt delayed retry")
		}
		return delay, err
	}

	consumerConnection := connect()
	defer consumerConnection.Close()
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPSCR")
	address := &pb.Address{Name: "sastest.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TPSCR.Consumer", Address: address, PrefetchCount: 5}
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, retryHandler, t)
	<-clientConnected

	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	err := produceMessages(producerConnection, pc, pctx, produceMessageCount, message)
	assert.Nil(t, err)

	msgCount := 0

	breakLoop := false
	for start := time.Now(); time.Since(start) < 15*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(5 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestProduceSingleConsumeNack(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 1
	produceMessageCount := 1
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)

	//retry handler
	retryHandler := func(msg *pb.Message) (int, error) {
		delay := 0
		err := errors.New("nack test")

		return delay, err
	}

	consumerConnection := connect()
	defer consumerConnection.Close()
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPSCN")
	address := &pb.Address{Name: "sastest.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TPSCN.Consumer", Address: address, PrefetchCount: 5}
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, retryHandler, t)
	<-clientConnected

	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	err := produceMessages(producerConnection, pc, pctx, produceMessageCount, message)
	assert.Nil(t, err)

	msgCount := 0

	breakLoop := true
	for start := time.Now(); time.Since(start) < 5*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(1 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestProduceManyConsumeMany(t *testing.T) {
	composeFile := "docker-compose.yml"
	rlSettings, err := GetRateLimitValues(t, composeFile)

	expectedMessageCount := 30
	if err == nil && rlSettings != nil && rlSettings.BucketSize > 0 {
		// By using the rate limit settings, we can also test that
		// publishing/consuming messages are _not_ rate limited
		expectedMessageCount = rlSettings.BucketSize * 3
	} else {
		t.Logf("Did not get rate limit settings, but testing can proceed: %v", err)
	}
	t.Logf("Expected message count: %d", expectedMessageCount)

	producerConnection := connect()
	defer producerConnection.Close()
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)

	consumerConnection := connect()
	defer consumerConnection.Close()
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPMCM")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TPMCM.Consumer", Address: address, PrefetchCount: 5}
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	err = produceMessages(producerConnection, pc, pctx, expectedMessageCount, message)
	assert.Nil(t, err)

	msgCount := 0

	breakLoop := false
	for start := time.Now(); time.Since(start) < 1*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(1 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestProduceFailsWithoutConnect(t *testing.T) {
	conn := connect()
	defer conn.Close()
	c := pb.NewProducerClient(conn)
	ctx := context.Background()
	defer c.Disconnect(ctx, &pb.Empty{})

	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPFWC")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}

	stream, _ := c.Publish(ctx)
	err := stream.Send(&pb.Message{Body: []byte("message"), Address: address, Persistent: true})
	assert.Nil(t, err)
	r, err := stream.Recv()
	assert.Nil(t, err)
	assert.NotNil(t, r)
	r, err = stream.Recv()
	assert.Nil(t, r)
	assert.Contains(t, err.Error(), "Could not find client identifier")
}

func TestConsumerDisconnectOKWithoutConnect(t *testing.T) {
	conn := connect()
	defer conn.Close()
	c := pb.NewConsumerClient(conn)
	ctx := context.Background()
	//defer conn.Close()

	r, err := c.Disconnect(ctx, &pb.Empty{})
	assert.Nil(t, err)
	assert.NotNil(t, r)
}

func TestProducerDisconnectOKWithoutConnect(t *testing.T) {
	conn := connect()
	defer conn.Close()
	c := pb.NewProducerClient(conn)
	ctx := context.Background()
	//defer conn.Close()

	r, err := c.Disconnect(ctx, &pb.Empty{})
	assert.Nil(t, err)
	assert.NotNil(t, r)
}

func TestProduceConsumeFiltersMatchAll(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 1
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

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
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)

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

	err := produceMessages(producerConnection, pc, pctx, 1, message)
	assert.Nil(t, err)

	message = &pb.Message{Body: []byte("mybody2"), Address: address, Headers: headers2}
	err = produceMessages(producerConnection, pc, pctx, 1, message)
	assert.Nil(t, err)

	message = &pb.Message{Body: []byte("mybody3"), Address: address, Headers: headers3}
	err = produceMessages(producerConnection, pc, pctx, 1, message)
	assert.Nil(t, err)

	message = &pb.Message{Body: []byte("mybody4"), Address: address, Headers: headers4}
	err = produceMessages(producerConnection, pc, pctx, 1, message) // this message is the one that gets consumed
	assert.Nil(t, err)

	msgCount := 0

	breakLoop := false
	for start := time.Now(); time.Since(start) < 1*time.Second; {
		select {
		case msg := <-messages:
			assert.Equal(t, msg.GetBody(), []byte("mybody4"))
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(1 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestProduceConsumeFiltersMatchAny(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 2
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

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
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)

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

	err := produceMessages(producerConnection, pc, pctx, 1, message) // this message gets consumed
	assert.Nil(t, err)

	message = &pb.Message{Body: []byte("mybody2"), Address: address, Headers: headers2}
	err = produceMessages(producerConnection, pc, pctx, 1, message)
	assert.Nil(t, err)

	message = &pb.Message{Body: []byte("mybody3"), Address: address, Headers: headers3}
	err = produceMessages(producerConnection, pc, pctx, 1, message)
	assert.Nil(t, err)

	message = &pb.Message{Body: []byte("mybody4"), Address: address, Headers: headers4}
	err = produceMessages(producerConnection, pc, pctx, 1, message) // this message get consumed
	assert.Nil(t, err)

	msgCount := 0

	breakLoop := false
	for start := time.Now(); time.Since(start) < 1*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(1 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestProduceSingleConsumeSingleCustomTopicName(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 1
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

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
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	done2 := make(chan bool)
	clientConnected2 := make(chan bool)
	consumerConnection2 := connect()
	source.Name = "sas.test.proxy.TPSCSCTN.Consumer2"
	c2 := pb.NewConsumerClient(consumerConnection2)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel2()
	defer c2.Disconnect(ctx2, &pb.Empty{})

	go consumeMessages(consumerConnection2, c2, ctx2, messages, done2, clientConnected2, source, defaultHandler, t)
	<-clientConnected2

	message := &pb.Message{Body: []byte("myreallycustommessage"), Address: address}

	err := produceMessages(producerConnection, pc, pctx, expectedMessageCount, message)
	assert.Nil(t, err)

	msgCount := 0

	messageUUIDs := make([]string, 0)

	breakLoop := false
	for start := time.Now(); time.Since(start) < 2*time.Second; {
		select {
		case msg := <-messages:
			messageUUIDs = append(messageUUIDs, msg.GetUuid())
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(2 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount*2, msgCount)
	assert.Equal(t, expectedMessageCount*2, len(messageUUIDs))
	assert.NotEmpty(t, messageUUIDs)
	assert.NotEqual(t, messageUUIDs[0], messageUUIDs[1])
}

func TestProduceSingleConsumeSingleCustomQueueName(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 1
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	consumerConnection := connect()
	defer consumerConnection.Close()
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TPSCSCQN")
	address := &pb.Address{Name: "sastest.direct", Subjects: subjects, Type: pb.Address_QUEUE}
	source := &pb.Source{Name: "sas.test.proxy.TPSCSCQN.Consumer", Address: address, PrefetchCount: 5}
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	err := produceMessages(producerConnection, pc, pctx, expectedMessageCount, message)
	assert.Nil(t, err)

	msgCount := 0

	breakLoop := false
	for start := time.Now(); time.Since(start) < 1*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(1 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestHeaders_Consume(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 1
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

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
	headers["traceparent"] = "00-traceid-spanid-flags"
	headers["tracestate"] = ""

	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TH")

	if providerType, ok := os.LookupEnv("SAS_BROKER_TYPE"); ok {
		if providerType == "azure" {
			delete(headers, "content-type")
			headers["RoutingKey"] = subjects[0]
		}
	}

	address := &pb.Address{Name: "sastest.direct", Subjects: subjects, Type: pb.Address_QUEUE}
	source := &pb.Source{Name: "sas.test.proxy.TH.Consumer", Address: address, PrefetchCount: 5}
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	message := &pb.Message{Body: []byte("mymessage"), Address: address, Headers: headers}

	err := produceMessages(producerConnection, pc, pctx, expectedMessageCount, message)
	assert.Nil(t, err)

	msgCount := 0

	received := make([]*pb.Message, 0)
	breakLoop := false
	for start := time.Now(); time.Since(start) < 1*time.Second; {
		select {
		case msg := <-messages:
			received = append(received, msg)
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(1 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
	assert.NotNil(t, received)
	assert.Greater(t, len(received), 0)
	assert.Contains(t, received[0].Headers, "traceparent")
	// remove the traceparent header from matching
	delete(headers, "traceparent")
	delete(received[0].Headers, "traceparent")
	assert.Equal(t, headers, received[0].Headers)
	assert.NotNil(t, received[0].GetAddress())
}

func TestProduceManyConsumeManyExclusive(t *testing.T) {
	t.Skip("This test is flaky. Skipping for now.")
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 20
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

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
	sourceName := fmt.Sprintf("sas.test.proxy.TPMCME.Consumer.%v", time.Now().Unix())
	source := &pb.Source{Name: sourceName, Address: address, PrefetchCount: 1, Exclusive: true}
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	done2 := make(chan bool)
	clientConnected2 := make(chan bool)
	consumerConnection2 := connect()
	c2 := pb.NewConsumerClient(consumerConnection2)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel2()
	defer c2.Disconnect(ctx2, &pb.Empty{})

	go consumeMessages(consumerConnection2, c2, ctx2, messages2, done2, clientConnected2, source, defaultHandler, t)
	<-clientConnected2

	message := &pb.Message{Body: []byte("myreallycustommessage"), Address: address}

	err := produceMessages(producerConnection, pc, pctx, expectedMessageCount, message)
	assert.Nil(t, err)

	msgCount := 0
	msgCount2 := 0

	messageUUIDs := make([]string, 0)
	messageUUIDs2 := make([]string, 0)

	breakLoop := false
	for start := time.Now(); time.Since(start) < 5*time.Second; {
		select {
		case msg := <-messages:
			messageUUIDs = append(messageUUIDs, msg.GetUuid())
			msgCount++
		case msg := <-messages2:
			messageUUIDs2 = append(messageUUIDs2, msg.GetUuid())
			msgCount2++
		case <-done:
			breakLoop = true
		case <-time.After(5 * time.Second):
			breakLoop = true
		}

		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
	assert.Equal(t, 0, msgCount2)
	assert.Equal(t, expectedMessageCount, len(messageUUIDs))
	assert.Equal(t, 0, len(messageUUIDs2))
	c2.Disconnect(ctx2, &pb.Empty{})
}

func TestConsumeMultiSubject(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	produceCount := 5
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

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
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	// Produce to binding one
	subjects = make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TSMS.1")
	address.Subjects = subjects
	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	err := produceMessages(producerConnection, pc, pctx, produceCount, message)
	assert.Nil(t, err)

	// Produce to binding 2
	subjects = make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TSMS.2")
	address.Subjects = subjects
	message = &pb.Message{Body: []byte("mymessage"), Address: address}

	err = produceMessages(producerConnection, pc, pctx, produceCount, message)
	assert.Nil(t, err)

	msgCount := 0

	breakLoop := false
	for start := time.Now(); time.Since(start) < 2*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(2 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, 2*produceCount, msgCount)
}

func TestProduceMultiSubject_FAIL(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	produceCount := 5
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

	// Publish with 2 subject bindings
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TSMS.1")
	subjects = append(subjects, "sas.test.proxy.TSMS.2")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	err := produceMessages(producerConnection, pc, pctx, produceCount, message)
	assert.NotNil(t, err)

	assert.Contains(t, err.Error(), "exactly one subject allowed in an Address")
}

func TestParentExchange_Consume(t *testing.T) {
	// Create a ParentAddress with name test.parent
	// Create an Address with name test.child and Parent ParentAddress
	// Consume messages from queue bound to test.child
	// Produce messages to test.parent

	producerConnection := connect()
	defer producerConnection.Close()
	produceCount := 5
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

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
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer func() {
		c.Disconnect(ctx, &pb.Empty{})
		//consumerConnection.Close()
	}()
	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	// adding some extra sleep in because of all the resources that need to be created in azure
	if providerType, ok := os.LookupEnv("SAS_BROKER_TYPE"); ok {
		switch providerType {
		case "azure":
			time.Sleep(6000 * time.Millisecond)
		default:
		}
	}

	// Publish to the parent address
	message := &pb.Message{Body: []byte("mymessage"), Address: parent}
	err := produceMessages(producerConnection, pc, pctx, produceCount, message)
	assert.Nil(t, err)

	msgCount := 0

	breakLoop := false
	for start := time.Now(); time.Since(start) < 1*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(2 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, produceCount, msgCount)
}

func TestAddressType_FAIL(t *testing.T) {
	t.Skip("This test is currently not valid because we ignore this error. Skipping for now.")
	// Create a ParentAddress with name test.parent
	// Create an Address with name test.child and Parent ParentAddress
	// Consume messages from queue bound to test.child
	// Produce messages to test.parent

	producerConnection := connect()
	defer producerConnection.Close()
	produceCount := 5
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})
	////defer producerConnection.Close()

	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TATF.1")

	parent := &pb.Address{Name: "test.parent", Type: 5, Subjects: subjects}

	message := &pb.Message{Body: []byte("mymessage"), Address: parent}
	err := produceMessages(producerConnection, pc, pctx, produceCount, message)
	assert.NotNil(t, err)
	if err != nil {
		assert.Contains(t, err.Error(), "5 is not a valid address type")
	}
}

func TestHeadersNoConsumeSubject(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 30
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})
	////defer producerConnection.Close()

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)

	consumerConnection := connect()
	defer consumerConnection.Close()
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.THNSS")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.THNSS.Consumer", Address: address, PrefetchCount: 5}
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	//defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	err := produceMessages(producerConnection, pc, pctx, expectedMessageCount, message)
	assert.Nil(t, err)

	msgCount := 0

	breakLoop := false
	for start := time.Now(); time.Since(start) < 1*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(1 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
	c.Disconnect(ctx, &pb.Empty{})
}

func TestSourceTwice(t *testing.T) {
	consumerConnection := connect()
	defer consumerConnection.Close()

	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TST")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TST.Consumer", Address: address, PrefetchCount: 5}
	c := pb.NewConsumerClient(consumerConnection)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	connConfig := connectConfig()

	_, _ = c.Connect(ctx, connConfig)

	stream, err := c.Consume(ctx)

	if err != nil {
		log.Panicf("could not subscribe: %v", err)
	}

	m := &pb.Consume{}
	m.Msg = &pb.Consume_Src{Src: source}
	stream.SendMsg(m)
	stream.SendMsg(m)
	msg, err := stream.Recv()
	assert.Equal(t, msg.GetMsg().GetError().GetMessage(), "Only one source message allowed per subscribe")
	assert.Nil(t, err)
}

func TestNoConnectionShareSameClientName(t *testing.T) {
	consumerConnection := connect()
	cc2 := connect()
	defer consumerConnection.Close()
	defer cc2.Close()

	c := pb.NewConsumerClient(consumerConnection)
	c2 := pb.NewConsumerClient(cc2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer c.Disconnect(ctx, &pb.Empty{})
	defer c2.Disconnect(ctx2, &pb.Empty{})
	defer cancel()
	defer cancel2()

	connConfig := connectConfig()
	connConfig.ClientName = "MyClientName"
	// Two connections, each connect. Should not interfere with each other
	_, err1 := c.Connect(ctx, connConfig)
	assert.Nil(t, err1)
	_, err2 := c2.Connect(ctx2, connConfig)
	assert.Nil(t, err2)
	// Demonstrate that calling connect twice on a single connection does not produce
	// an error, Arke should ignore the second Connect() call
	_, err2 = c2.Connect(ctx2, connConfig)
	assert.Nil(t, err2)
}

func TestConsumeNoAckReconnectConsume(t *testing.T) {
	t.Skip("This test is flaky. Skipping for now.")
	expectedMsgBodyUUID := uuid.New().String()

	// Set up the consumer
	// Produce a single message
	// Consume the message but do not ack/nack
	// Disconnect consumer
	// Connect new consumer
	// Consume message
	// Make sure it's the correct one

	consumerConnection := connect()
	defer consumerConnection.Close()

	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TCNARC")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	// use a unique source name so we don't consume messages from a failed test
	source := &pb.Source{Name: "sas.test.proxy.TCNARC.Consumer-" + expectedMsgBodyUUID, Address: address, PrefetchCount: 5}
	c := pb.NewConsumerClient(consumerConnection)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	connConfig := connectConfig()
	connConfig.ClientName = "consumer1"

	_, _ = c.Connect(ctx, connConfig)

	stream, err := c.Consume(ctx)

	assert.Nil(t, err)
	if err != nil {
		fmt.Println("could not subscribe:", err)
	}

	m := &pb.Consume{}
	m.Msg = &pb.Consume_Src{Src: source}

	stream.SendMsg(m)

	// After we send the consume message, produce a single message
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 1
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	message := &pb.Message{Body: []byte(expectedMsgBodyUUID), Address: address}

	go func() {
		err := produceMessages(producerConnection, pc, pctx, expectedMessageCount, message)
		assert.Nil(t, err)
		if err != nil {
			fmt.Println("err producing messages:", err)
		}
		pc.Disconnect(pctx, &pb.Empty{})
		producerConnection.Close()
	}()

	// Receive the message
	resp, err := stream.Recv()
	assert.Nil(t, err)
	msgBody := string(resp.GetMsg().GetBody())
	assert.Equal(t, expectedMsgBodyUUID, msgBody)
	// Disconnect before ack/nack
	cancel()
	c.Disconnect(ctx, &pb.Empty{})
	consumerConnection.Close()

	// Create a new consumer
	consumerConnection2 := connect()
	c2 := pb.NewConsumerClient(consumerConnection2)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	defer c2.Disconnect(ctx2, &pb.Empty{})
	connConfig.ClientName = "consumer2"
	_, _ = c2.Connect(ctx2, connConfig)

	defer func() {
		cancel2()
		c2.Disconnect(ctx2, &pb.Empty{})
		consumerConnection2.Close()
	}()

	stream2, err := c2.Consume(ctx2)
	assert.Nil(t, err)
	if err != nil {
		fmt.Println("could not subscribe:", err)
	}
	// Consume message and make sure it is correct
	stream2.SendMsg(m)
	resp2, err := stream2.Recv()
	assert.Nil(t, err)
	msgBody2 := string(resp2.GetMsg().GetBody())
	assert.Equal(t, expectedMsgBodyUUID, msgBody2)
	ret := &pb.Consume{Msg: &pb.Consume_Ack{Ack: &pb.MessageConsumed{Uuid: resp2.GetMsg().GetUuid()}}}
	err = stream2.Send(ret)
	assert.Nil(t, err)

	assert.Equal(t, msgBody, msgBody2)
}

func TestConsumeDeleteBinding(t *testing.T) {
	// create a consumer with multiple bindings, stop consuming
	// wait a second
	// create the same consumer but with less bindings
	// publish a message with one subject from consumer 2
	// publish another message with the removed binding from consumer 1
	// ensure we only get the one message from the consumer 2 bindings
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 1
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)

	// set up consumer 1 and subscribe with 2 subjects, then disconnect
	consumerConnection := connect()
	defer consumerConnection.Close()
	defer consumerConnection.Close()
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TCDB.1")
	subjects = append(subjects, "sas.test.proxy.TCDB.2")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TCDB.Consumer", Address: address, PrefetchCount: 5}
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected
	cancel()
	_, _ = c.Disconnect(ctx, &pb.Empty{})

	// set up consumer 1 and subscribe with 2 subjects, then disconnect
	consumerConnection = connect()
	subjects = []string{"sas.test.proxy.TCDB.2"}
	address.Subjects = subjects
	source.Address = address
	c = pb.NewConsumerClient(consumerConnection)
	ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})
	defer consumerConnection.Close()
	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	// publish to sas.test.proxy.TCDB.2, then to sas.test.proxy.TCBD.1
	subjects = []string{"sas.test.proxy.TCDB.2"}
	address.Subjects = subjects
	source.Address = address
	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	err := produceMessages(producerConnection, pc, pctx, expectedMessageCount, message)
	assert.Nil(t, err)

	subjects = []string{"sas.test.proxy.TCDB.1"}
	address.Subjects = subjects
	source.Address = address
	message = &pb.Message{Body: []byte("mymessage"), Address: address}

	err = produceMessages(producerConnection, pc, pctx, expectedMessageCount, message)
	assert.Nil(t, err)

	msgCount := 0

	breakLoop := false
	for start := time.Now(); time.Since(start) < 5*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(1 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestDeadLettering(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 1
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()

	messages1 := make(chan *pb.Message)

	done1 := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	consumerConnection1 := connect()
	deadLetterExchange := "sas.dlq"
	deadLetterSubject := "deadlettersubject"
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TDL")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TDL.Consumer", Address: address, PrefetchCount: 1}
	source.Options = make(map[string]string)
	source.Options["DeadLetterAddress"] = deadLetterExchange
	source.Options["DeadLetterSubject"] = deadLetterSubject
	source.Options["MessageTTL"] = "5000"

	// connect a consumer and then disconnect then disconnect
	c1 := pb.NewConsumerClient(consumerConnection1)
	ctx := context.Background()
	go consumeMessages(consumerConnection1, c1, ctx, messages1, done1, clientConnected, source, defaultHandler, t)
	<-clientConnected
	time.Sleep(5 * time.Second)
	c1.Disconnect(ctx, &pb.Empty{})
	consumerConnection1.Close()

	message := &pb.Message{Body: []byte("mymessage"), Address: address}
	// produce message, wait for it to DLQ, then consume it from the DLQ
	err := produceMessages(producerConnection, pc, pctx, expectedMessageCount, message)
	assert.Nil(t, err)
	pc.Disconnect(pctx, &pb.Empty{})
	producerConnection.Close()

	// sleep for a few seconds to ensure that we dead letter due to message expiration
	time.Sleep(10 * time.Second)

	done2 := make(chan bool)
	messages2 := make(chan *pb.Message)
	clientConnected2 := make(chan bool)
	consumerConnection2 := connect()
	// connect a consumer to the DLQ and consume
	c2 := pb.NewConsumerClient(consumerConnection2)
	ctx = context.Background()
	defer func() {
		c2.Disconnect(ctx, &pb.Empty{})
		consumerConnection2.Close()
	}()

	source.Address.Name = deadLetterExchange
	source.Options = make(map[string]string)
	source.Name = "sas.test.proxy.TDL.Consumer.dlq"
	source.Type = pb.Source_TEMPORARY
	subjects = append(subjects, "deadlettersubject")
	source.Address.Subjects = subjects
	go consumeMessages(consumerConnection2, c2, ctx, messages2, done2, clientConnected2, source, defaultHandler, t)
	<-clientConnected2

	msgCount := 0

	for start := time.Now(); time.Since(start) < 30*time.Second; {
		select {
		case <-messages2:
			msgCount++
		case <-time.After(10 * time.Second):
		}
		if msgCount == expectedMessageCount {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestDeadLetteringReject(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 1
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()

	messages1 := make(chan *pb.Message)

	done1 := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	// reject handler
	rejectHandler := func(msg *pb.Message) (int, error) {
		delay := 0
		err := fmt.Errorf("Reject message!")

		return delay, err
	}

	consumerConnection1 := connect()
	deadLetterExchange := "sas.dlq"
	deadLetterSubject := "deadlettersubject"
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TDL")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TDL.Consumer", Address: address, PrefetchCount: 1}
	source.Options = make(map[string]string)
	source.Options["DeadLetterAddress"] = deadLetterExchange
	source.Options["DeadLetterSubject"] = deadLetterSubject
	source.Options["MessageTTL"] = "5000"

	// connect a consumer and then disconnect then disconnect
	c1 := pb.NewConsumerClient(consumerConnection1)
	ctx := context.Background()
	go consumeMessages(consumerConnection1, c1, ctx, messages1, done1, clientConnected, source, rejectHandler, t)
	<-clientConnected
	time.Sleep(5 * time.Second)
	defer func() {
		c1.Disconnect(ctx, &pb.Empty{})
		consumerConnection1.Close()
	}()

	message := &pb.Message{Body: []byte("mymessage"), Address: address}
	// produce message, wait for it to DLQ, then consume it from the DLQ
	err := produceMessages(producerConnection, pc, pctx, expectedMessageCount, message)
	assert.Nil(t, err)
	pc.Disconnect(pctx, &pb.Empty{})
	producerConnection.Close()

	done2 := make(chan bool)
	messages2 := make(chan *pb.Message)
	clientConnected2 := make(chan bool)
	consumerConnection2 := connect()
	// connect a consumer to the DLQ and consume
	c2 := pb.NewConsumerClient(consumerConnection2)
	ctx = context.Background()
	defer func() {
		c2.Disconnect(ctx, &pb.Empty{})
		consumerConnection2.Close()
	}()

	source.Address.Name = deadLetterExchange
	source.Options = make(map[string]string)
	source.Name = "sas.test.proxy.TDL.Consumer.dlq"
	subjects = append(subjects, "deadlettersubject")
	source.Type = pb.Source_TEMPORARY
	source.Address.Subjects = subjects
	go consumeMessages(consumerConnection2, c2, ctx, messages2, done2, clientConnected2, source, defaultHandler, t)
	<-clientConnected2

	msgCount := 0

	for start := time.Now(); time.Since(start) < 30*time.Second; {
		select {
		case <-messages2:
			msgCount++
		case <-time.After(10 * time.Second):
		}
		if msgCount == expectedMessageCount {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestDeadLetteringRejectAutoDelete(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 1
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()

	messages1 := make(chan *pb.Message)

	done1 := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	// reject handler
	rejectHandler := func(msg *pb.Message) (int, error) {
		delay := 0
		err := fmt.Errorf("Reject message!")

		return delay, err
	}

	testUUID := uuid.New().String()

	consumerConnection1 := connect()
	deadLetterExchange := "sas.dlq"
	deadLetterSubject := "deadlettersubject." + testUUID
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TDL."+testUUID)
	srcName := "sas.test.proxy.TDL.Consumer." + testUUID
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: srcName, Address: address, PrefetchCount: 1, AutoDelete: true}
	source.Options = make(map[string]string)
	source.Options["DeadLetterAddress"] = deadLetterExchange
	source.Options["DeadLetterSubject"] = deadLetterSubject
	source.Options["MessageTTL"] = "5000"

	// connect a consumer and then disconnect then disconnect
	c1 := pb.NewConsumerClient(consumerConnection1)
	ctx := context.Background()
	go consumeMessages(consumerConnection1, c1, ctx, messages1, done1, clientConnected, source, rejectHandler, t)
	<-clientConnected
	time.Sleep(5 * time.Second)
	defer func() {
		c1.Disconnect(ctx, &pb.Empty{})
		consumerConnection1.Close()
	}()

	message := &pb.Message{Body: []byte("mymessage"), Address: address}
	// produce message, wait for it to DLQ, then consume it from the DLQ
	err := produceMessages(producerConnection, pc, pctx, expectedMessageCount, message)
	assert.Nil(t, err)
	pc.Disconnect(pctx, &pb.Empty{})
	producerConnection.Close()

	done2 := make(chan bool)
	messages2 := make(chan *pb.Message)
	clientConnected2 := make(chan bool)
	consumerConnection2 := connect()
	// connect a consumer to the DLQ and consume
	c2 := pb.NewConsumerClient(consumerConnection2)
	ctx = context.Background()
	defer func() {
		c2.Disconnect(ctx, &pb.Empty{})
		consumerConnection2.Close()
	}()

	source.Address.Name = deadLetterExchange
	source.Options = make(map[string]string)
	source.Name = srcName + ".dlq"
	source.Type = pb.Source_TEMPORARY
	subjects = append(subjects, deadLetterSubject)
	source.Address.Subjects = subjects
	go consumeMessages(consumerConnection2, c2, ctx, messages2, done2, clientConnected2, source, defaultHandler, t)
	<-clientConnected2

	msgCount := 0

	for start := time.Now(); time.Since(start) < 30*time.Second; {
		select {
		case <-messages2:
			msgCount++
		case <-time.After(10 * time.Second):
		}
		if msgCount == expectedMessageCount {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestNoSubjectNoBinding(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 0
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	consumerConnection := connect()
	defer consumerConnection.Close()
	// No subject should result in no binding between Address and Source,
	// therefore it should produce no messages
	subjects := make([]string, 0)
	consumeAddress := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	subjects = append(subjects, "sas.test.proxy.TNSNB")
	produceAddress := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TNSNB.Consumer", Address: consumeAddress, PrefetchCount: 1}
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	message := &pb.Message{Body: []byte("mymessage"), Address: produceAddress}

	err := produceMessages(producerConnection, pc, pctx, 1, message)
	assert.Nil(t, err)

	msgCount := 0

	breakLoop := false
	for start := time.Now(); time.Since(start) < 2*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(2 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestNoSubjectWithFilters(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 1
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)

	consumerConnection := connect()
	defer consumerConnection.Close()
	source := &pb.Source{Name: "sas.test.proxy.TPCFMAll", PrefetchCount: 5}
	subjects := make([]string, 0)
	consumerAddress := &pb.Address{Name: "sastest.headers", Subjects: subjects, Type: pb.Address_FILTER}
	subjects = append(subjects, "sas.test.proxy.TPCFMAll")
	producerAddress := &pb.Address{Name: "sastest.headers", Subjects: subjects, Type: pb.Address_FILTER}
	filter := &pb.Filter{Type: pb.Filter_ALL}
	matches := make([]*pb.Match, 0)
	matches = append(matches, &pb.Match{Name: "HeaderToMatchAll", Value: "MyFancyValue"})
	matches = append(matches, &pb.Match{Name: "AnotherHeaderToMatchAll", Value: "MyFancyValue"})
	filter.Matches = matches
	source.Filters = make([]*pb.Filter, 0)
	source.Filters = append(source.Filters, filter)
	source.Address = consumerAddress
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)

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

	message := &pb.Message{Body: []byte("mybody1"), Address: producerAddress, Headers: headers1}

	err := produceMessages(producerConnection, pc, pctx, 1, message)
	assert.Nil(t, err)

	message = &pb.Message{Body: []byte("mybody2"), Address: producerAddress, Headers: headers2}
	err = produceMessages(producerConnection, pc, pctx, 1, message)
	assert.Nil(t, err)

	message = &pb.Message{Body: []byte("mybody3"), Address: producerAddress, Headers: headers3}
	err = produceMessages(producerConnection, pc, pctx, 1, message)
	assert.Nil(t, err)

	message = &pb.Message{Body: []byte("mybody4"), Address: producerAddress, Headers: headers4}
	err = produceMessages(producerConnection, pc, pctx, 1, message) // this message is the one that gets consumed
	assert.Nil(t, err)

	msgCount := 0

	breakLoop := false
	for start := time.Now(); time.Since(start) < 1*time.Second; {
		select {
		case msg := <-messages:
			assert.Equal(t, msg.GetBody(), []byte("mybody4"))
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(1 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestSubscribeAckNackInvalidID(t *testing.T) {
	consumerConnection := connect()
	defer consumerConnection.Close()

	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TAIID")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TAIID", Address: address, PrefetchCount: 5}
	c := pb.NewConsumerClient(consumerConnection)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	connConfig := connectConfig()
	authResp, connErr := c.Connect(ctx, connConfig)
	if connErr != nil {
		log.Panicf("could not authenticate: %v", connErr)
	}
	if !authResp.GetSuccess() {
		log.Panicf("could not authenticate: %v", authResp.GetError().GetMessage())
	}

	stream, err := c.Consume(ctx)
	if err != nil {
		log.Panicf("could not subscribe: %v", err)
	}

	m := &pb.Consume{}
	m.Msg = &pb.Consume_Src{Src: source}
	err = stream.SendMsg(m)
	assert.Nil(t, err)
	defer stream.CloseSend()

	// Ack
	ret := &pb.Consume{Msg: &pb.Consume_Ack{Ack: &pb.MessageConsumed{Nack: false, RequeueDelay: int32(5), Uuid: "54321"}}}
	err = stream.Send(ret)
	assert.Nil(t, err)

	msg, err := stream.Recv()
	assert.Nil(t, err)
	assert.Equal(t, "No message with uuid 54321", msg.GetConsumedResponse().GetError().GetMessage())
	assert.False(t, msg.GetConsumedResponse().GetSuccess())

	// Nack with retry
	ret = &pb.Consume{Msg: &pb.Consume_Ack{Ack: &pb.MessageConsumed{Nack: true, RequeueDelay: int32(5), Uuid: "54321"}}}
	err = stream.Send(ret)
	assert.Nil(t, err)

	msg, err = stream.Recv()
	assert.Nil(t, err)
	assert.Equal(t, "No message with uuid 54321", msg.GetConsumedResponse().GetError().GetMessage())
	assert.False(t, msg.GetConsumedResponse().GetSuccess())

	// Nack with retry
	ret = &pb.Consume{Msg: &pb.Consume_Ack{Ack: &pb.MessageConsumed{Nack: true, RequeueDelay: int32(0), Uuid: "54321"}}}
	err = stream.Send(ret)
	assert.Nil(t, err)

	msg, err = stream.Recv()
	assert.Nil(t, err)
	assert.Equal(t, "No message with uuid 54321", msg.GetConsumedResponse().GetError().GetMessage())
	assert.False(t, msg.GetConsumedResponse().GetSuccess())
}

func TestSubscribeAckInvalidIDNoConnect(t *testing.T) {
	consumerConnection := connect()
	defer consumerConnection.Close()
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.TAIIDNC")
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{Name: "sas.test.proxy.TAIIDNC", Address: address, PrefetchCount: 5}
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	connConfig := connectConfig()
	authResp, connErr := c.Connect(ctx, connConfig)
	if connErr != nil {
		log.Panicf("could not authenticate: %v", connErr)
	}
	if !authResp.GetSuccess() {
		log.Panicf("could not authenticate: %v", authResp.GetError().GetMessage())
	}
	stream, err := c.Consume(ctx)
	if err != nil {
		log.Panicf("could not subscribe: %v", err)
	}
	m := &pb.Consume{}
	m.Msg = &pb.Consume_Src{Src: source}
	err = stream.SendMsg(m)
	assert.Nil(t, err)
	defer stream.CloseSend()

	// Need to make sure we have finished subscribing
	time.Sleep(1 * time.Second)
	c.Disconnect(ctx, &pb.Empty{})
	//time.Sleep(1000)

	// Ack
	ret := &pb.Consume{Msg: &pb.Consume_Ack{Ack: &pb.MessageConsumed{Nack: false, RequeueDelay: int32(5), Uuid: "54321"}}}
	err = stream.Send(ret)
	assert.Nil(t, err)
	msg, err := stream.Recv()
	assert.Nil(t, err)
	assert.True(t, strings.HasPrefix(msg.GetConsumedResponse().GetError().GetMessage(), "could not retrieve broker details for this connection"))
	assert.False(t, msg.GetConsumedResponse().GetSuccess())

	// Nack with retry
	ret = &pb.Consume{Msg: &pb.Consume_Ack{Ack: &pb.MessageConsumed{Nack: true, RequeueDelay: int32(5), Uuid: "54321"}}}
	err = stream.Send(ret)
	assert.Nil(t, err)
	msg, err = stream.Recv()
	assert.Nil(t, err)
	assert.True(t, strings.HasPrefix(msg.GetConsumedResponse().GetError().GetMessage(), "could not retrieve broker details for this connection"))
	assert.False(t, msg.GetConsumedResponse().GetSuccess())

	// Nack with retry
	ret = &pb.Consume{Msg: &pb.Consume_Ack{Ack: &pb.MessageConsumed{Nack: true, RequeueDelay: int32(0), Uuid: "54321"}}}
	err = stream.Send(ret)
	assert.Nil(t, err)
	msg, err = stream.Recv()
	assert.Nil(t, err)
	assert.True(t, strings.HasPrefix(msg.GetConsumedResponse().GetError().GetMessage(), "could not retrieve broker details for this connection"))
	assert.False(t, msg.GetConsumedResponse().GetSuccess())
}

func TestRateLimits(t *testing.T) {

	// Collect rate limit values either from docker-compose or the env vars
	composeFile := "docker-compose.yml"
	rlSettings, err := GetRateLimitValues(t, composeFile)
	if err != nil {
		t.Skipf("rate values not available: %v", err)
	}
	t.Logf("rate limit: %+v", rlSettings)

	conn := connect()
	assert.NotNil(t, conn, "should get a connection")
	c := pb.NewConsumerClient(conn)
	ctx := context.Background()
	defer c.Disconnect(ctx, &pb.Empty{})
	defer conn.Close()
	connConfig := connectConfig()

	// Use all tokens in the bucket
	t.Run("Connect", func(t *testing.T) {
		for i := 0; i < rlSettings.BucketSize; i++ {
			// Attempt a non-TLS connection to arke first
			_, err = c.Connect(ctx, connConfig)
			assert.Nil(t, err, "should not get an error connecting")
		}
	})

	// Attempt to connect with no tokens left in the bucket
	t.Run("ConnectExceedBucketSize", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			_, err = c.Connect(ctx, connConfig)
			if err != nil {
				break
			}
		}
		assert.NotNil(t, err, "should get an error when exceeding bucket size")
	})
}
