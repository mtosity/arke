package azure

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sassoftware.io/viya/arke/internal/util"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	azadmin "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	pb "sassoftware.io/viya/arke/api"
)

func init() {
}

type azureClientMock struct {
	mock.Mock
	azureClientShim //nolint:unused
	blockConnect    time.Duration
	Receives        []*azureMsgMock
}

type azureSenderMock struct {
	mock.Mock
	azureSenderShim //nolint:unused
}

type MockRecv struct {
	Message *azureMsgMock
	Error   error
}

type azureMsgMock struct {
	mock.Mock
	azureMessageShim
	properties map[string]interface{}
}

// CLIENT

func (m *azureClientMock) NewSender(string) (azureSenderShim, error) {
	args := m.Called()
	rec := args.Get(0).(*azureSenderMock)
	return rec, args.Error(1)
}

func (m *azureClientMock) CreateTopic(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	err := args.Error(0)
	return err
}

func (m *azureClientMock) Connect() error {
	args := m.Called()
	return args.Error(0)
}

func (m *azureClientMock) SetSender(*azureSenderMock) error {
	args := m.Called()
	return args.Error(0)
}

func (m *azureClientMock) GenerateForwardToName(string) string {
	args := m.Called()
	return args.String(0)
}

func (m *azureClientMock) CreateSubscription(context.Context, string, string, *azadmin.CreateSubscriptionOptions) error {
	args := m.Called()
	return args.Error(0)
}

func (m *azureClientMock) ListRules(string, string) ([]azadmin.RuleProperties, error) {
	args := m.Called()
	re := args.Get(0).([]azadmin.RuleProperties)
	return re, args.Error(1)
}

func (m *azureClientMock) DeleteRule(ctx context.Context, topicName, subName, ruleName string) error {
	args := m.Called(ctx, topicName, subName, ruleName)
	return args.Error(0)
}

func (m *azureClientMock) CreateRule(ctx context.Context, topicName, subName, ruleName, ruleText string) error {
	args := m.Called(ctx, topicName, subName, ruleName, ruleText)
	return args.Error(0)
}

func (m *azureClientMock) UpdateRule(ctx context.Context, topicName, subName, ruleName, ruleText string) error {
	args := m.Called(ctx, topicName, subName, ruleName, ruleText)
	return args.Error(0)
}

// SENDER

func (m *azureSenderMock) ScheduleMessage(context.Context, *azservicebus.Message, time.Time) ([]int64, error) {
	args := m.Called()
	return nil, args.Error(1)
}

func (m *azureSenderMock) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *azureSenderMock) SendMessage(context.Context, *azservicebus.Message) error {
	args := m.Called()
	return args.Error(0)
}

// RECEIVER

func (m *azureClientMock) ReceiveMessages(ctx context.Context, topicName, subscriptionName string, prefetch int, messageChannel chan azureMessageShim, deadLetterEnabled bool) error {
	args := m.Called(ctx, topicName, subscriptionName, prefetch, messageChannel, deadLetterEnabled)

	for _, msg := range m.Receives {
		messageChannel <- msg
	}
	return args.Error(0)
}

// MESSAGE

func (m *azureMsgMock) DeliveryCount() uint32 {
	args := m.Called()
	return uint32(args.Int(0))
}

func (m *azureMsgMock) ID() string {
	args := m.Called()
	return args.String(0)
}

func (m *azureMsgMock) ContentType() string {
	args := m.Called()
	return args.String(0)
}

func (m *azureMsgMock) Data() []byte {

	args := m.Called()
	return []byte(fmt.Sprintf("%s", args.Get(0)))
}

func (m *azureMsgMock) Schedule(context.Context, time.Time) error {
	args := m.Called()
	return args.Error(0)
}

func (m *azureMsgMock) Send(context.Context) error {

	args := m.Called()
	return args.Error(0)
}

func (m *azureMsgMock) SetLockToken([16]byte)                {}
func (m *azureMsgMock) SetData([]byte)                       {}
func (m *azureMsgMock) SetProperties(map[string]interface{}) {}
func (m *azureMsgMock) SetProperty(string, interface{})      {}
func (m *azureMsgMock) SetContentType(string)                {}

func (m *azureMsgMock) Properties() map[string]interface{} {
	args := m.Called()
	return args.Get(0).(map[string]interface{})
}

func (m *azureMsgMock) Property(key string) (interface{}, bool) {
	val, ok := m.properties[key]
	return val, ok
}

func (m *azureMsgMock) Ack(context.Context) error {
	args := m.Called()
	return args.Error(0)
}

func (m *azureMsgMock) DeadLetter(context.Context) error {
	args := m.Called()
	return args.Error(0)
}

func (m *azureMsgMock) Nack(ctx context.Context, deadLetterEnabled bool) error {
	args := m.Called(ctx, deadLetterEnabled)
	return args.Error(0)
}

func (m *azureMsgMock) ClientSentTime() time.Time {
	return time.Now()
}

func (m *azureMsgMock) SetClientSentTime() {}

func TestNewAzureProvider(t *testing.T) {
	prov := NewAzureProvider()
	assert.NotNil(t, prov)
}

func TestConnect(t *testing.T) {
	prov := NewAzureProvider()
	cc := &pb.ConnectionConfiguration{}
	ctx := context.Background()
	err := prov.Connect(ctx, cc, false)
	assert.NotNil(t, err)
}

func Test_Connect_NoClient(t *testing.T) {
	prov := NewAzureProvider()
	assert.NotNil(t, prov)

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "", errors.New("noclient")
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)

	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "noclient")
}

func TestConnect_Stats(t *testing.T) {
	prov := NewAzureProvider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureClientMock{}
	amock.On("Connect").Return(nil)
	oldNewAzureClient := NewAZClient

	NewAZClient = func(string, string, string) azureClientShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAZClient = oldNewAzureClient
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	stats := prov.Stats()
	assert.Equal(t, len(stats.Clients), 1)
	client := stats.Clients[0]
	assert.Equal(t, client.Streams, 0)
	assert.Equal(t, client.ActiveMessages, 0)
	assert.Equal(t, client.Produced, 0)
	assert.Equal(t, client.Consumed, 0)
	assert.Equal(t, client.ID, "1234")

	amock.AssertExpectations(t)
}

func Test_Ack_NoMsg(t *testing.T) {
	prov := NewAzureProvider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureClientMock{}
	amock.On("Connect").Return(nil)

	oldNewAzureClient := NewAZClient

	NewAZClient = func(string, string, string) azureClientShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAZClient = oldNewAzureClient
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)
	msg := pb.Message{}
	err = prov.Ack(ctx, msg.GetUuid())
	assert.Contains(t, err.GetMessage(), "No message with uuid")

	amock.AssertExpectations(t)
}

func Test_Nack_NoMsg(t *testing.T) {
	prov := NewAzureProvider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureClientMock{}
	amock.On("Connect").Return(nil)

	oldNewAzureClient := NewAZClient

	NewAZClient = func(string, string, string) azureClientShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAZClient = oldNewAzureClient
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)
	msg := pb.Message{}
	err = prov.Nack(ctx, msg.GetUuid())
	assert.Contains(t, err.GetMessage(), "No message with uuid")

	amock.AssertExpectations(t)
}

func Test_Ack(t *testing.T) {
	prov := NewAzureProvider()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureClientMock{}
	amock.On("ReceiveMessages", mock.Anything,
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("int"),
		mock.AnythingOfType("chan azure.azureMessageShim"),
		mock.AnythingOfType("bool")).Return(nil)

	amock.Receives = make([]*azureMsgMock, 0)
	nmsg := &azureMsgMock{}
	props := make(map[string]interface{})
	props["header1"] = "value"
	nmsg.On("Properties").Return(props)
	nmsg.On("ContentType").Return("json")
	nmsg.On("Data").Return("hello")
	nmsg.On("Ack").Return(nil)

	amock.Receives = append(amock.Receives, nmsg)

	amock.On("CreateSubscription").Return(nil)
	rules := make([]azadmin.RuleProperties, 0)
	defaultRule := azadmin.RuleProperties{
		Name: "$Default",
	}
	rules = append(rules, defaultRule)
	amock.On("ListRules").Return(rules, nil)
	amock.On("DeleteRule", mock.Anything, "topicName", sourceNameToSubName("subName"), "$Default").Return(nil).Once()

	amock.On("CreateRule",
		mock.Anything,
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		"RoutingAndFilterRule00",
		"RoutingKey = 'one'",
	).Return(nil).Once()
	amock.On("CreateRule",
		mock.Anything,
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		"RoutingAndFilterRule01",
		"RoutingKey = 'two'",
	).Return(nil).Once()

	amock.On("Connect").Return(nil)
	amock.On("CreateTopic", mock.Anything, "topicName").Return(nil).Once()

	oldNewAzureClient := NewAZClient

	NewAZClient = func(string, string, string) azureClientShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAZClient = oldNewAzureClient
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Address: &pb.Address{Name: "topicName", Subjects: []string{"one", "two"}}, Name: "subName"}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc
	time.Sleep(100 * time.Millisecond)
	err = prov.Ack(ctx, msg.GetUuid())
	assert.NotNil(t, msg)
	assert.Nil(t, err)

	amock.AssertExpectations(t)
	nmsg.AssertExpectations(t)
}

func Test_Ack_AckErr(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureClientMock{}
	amock.Receives = make([]*azureMsgMock, 0)
	amock.On("ReceiveMessages", mock.Anything,
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("int"),
		mock.AnythingOfType("chan azure.azureMessageShim"),
		mock.AnythingOfType("bool")).Return(nil)

	nmsg := &azureMsgMock{}
	props := make(map[string]interface{})
	props["header1"] = "value"
	nmsg.On("Properties").Return(props)
	nmsg.On("ContentType").Return("json")
	nmsg.On("Data").Return("hello")
	nmsg.On("Ack").Return(errors.New("AckError"))

	amock.Receives = append(amock.Receives, nmsg)
	amock.On("CreateSubscription").Return(nil)

	rules := make([]azadmin.RuleProperties, 0)
	defaultRule := azadmin.RuleProperties{
		Name: "$Default",
	}
	rules = append(rules, defaultRule)
	amock.On("ListRules").Return(rules, nil)
	amock.On("DeleteRule", mock.Anything, "topicName", sourceNameToSubName("subName"), "$Default").Return(nil).Once()

	amock.On("CreateRule",
		mock.Anything,
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		"RoutingAndFilterRule00",
		"RoutingKey = 'one'",
	).Return(nil).Once()
	amock.On("CreateRule",
		mock.Anything,
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		"RoutingAndFilterRule01",
		"RoutingKey = 'two'",
	).Return(nil).Once()

	amock.On("Connect").Return(nil)
	amock.On("CreateTopic", mock.Anything, "topicName").Return(nil).Once()

	oldNewAzureClient := NewAZClient

	NewAZClient = func(string, string, string) azureClientShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAZClient = oldNewAzureClient
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Address: &pb.Address{Name: "topicName", Subjects: []string{"one", "two"}}, Name: "subName"}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc
	assert.NotNil(t, msg)
	time.Sleep(100 * time.Millisecond)
	err = prov.Ack(ctx, msg.GetUuid())
	assert.NotNil(t, err)
	assert.Equal(t, err.GetMessage(), "AckError")

	amock.AssertExpectations(t)
	nmsg.AssertExpectations(t)
}

func Test_Nack(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureClientMock{}
	amock.On("ReceiveMessages", mock.Anything,
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("int"),
		mock.AnythingOfType("chan azure.azureMessageShim"),
		mock.AnythingOfType("bool")).Return(nil)

	amock.Receives = make([]*azureMsgMock, 0)
	nmsg := &azureMsgMock{}
	props := make(map[string]interface{})
	props["header1"] = "value"
	nmsg.On("Properties").Return(props)
	nmsg.On("ContentType").Return("json")
	nmsg.On("Data").Return("hello")
	nmsg.On("Nack", mock.Anything, false).Return(nil)

	amock.Receives = append(amock.Receives, nmsg)
	amock.On("CreateSubscription").Return(nil)

	rules := make([]azadmin.RuleProperties, 0)
	amock.On("ListRules").Return(rules, nil)

	amock.On("Connect").Return(nil)
	amock.On("CreateTopic", ctx, "topicName").Return(nil)

	oldNewAzureClient := NewAZClient

	NewAZClient = func(string, string, string) azureClientShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAZClient = oldNewAzureClient
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Address: &pb.Address{Name: "topicName"}}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc
	time.Sleep(100 * time.Millisecond)
	err = prov.Nack(ctx, msg.GetUuid())
	assert.NotNil(t, msg)
	assert.Nil(t, err)

	amock.AssertExpectations(t)
	nmsg.AssertExpectations(t)
}

func Test_DeadLetter(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureClientMock{}
	amock.On("ReceiveMessages", mock.Anything,
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("int"),
		mock.AnythingOfType("chan azure.azureMessageShim"),
		mock.AnythingOfType("bool")).Return(nil)

	amock.Receives = make([]*azureMsgMock, 0)
	nmsg := &azureMsgMock{}
	props := make(map[string]interface{})
	props["header1"] = "value"
	nmsg.On("Properties").Return(props)
	nmsg.On("ContentType").Return("json")
	nmsg.On("Data").Return("hello")
	nmsg.On("DeadLetter").Return(nil)

	amock.Receives = append(amock.Receives, nmsg)
	amock.On("CreateSubscription").Return(nil)

	rules := make([]azadmin.RuleProperties, 0)
	amock.On("ListRules").Return(rules, nil)

	amock.On("Connect").Return(nil)
	amock.On("CreateTopic", ctx, "topicName").Return(nil)

	oldNewAzureClient := NewAZClient

	NewAZClient = func(string, string, string) azureClientShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAZClient = oldNewAzureClient
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Address: &pb.Address{Name: "topicName"}}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc
	time.Sleep(100 * time.Millisecond)
	err = prov.DeadLetter(ctx, src, msg.GetUuid())
	assert.NotNil(t, msg)
	assert.Nil(t, err)

	amock.AssertExpectations(t)
}

func Test_DeadLetterNoMsg(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureClientMock{}
	amock.Receives = make([]*azureMsgMock, 0)
	nmsg := &azureMsgMock{}

	amock.On("Connect").Return(nil)

	oldNewAzureClient := NewAZClient

	NewAZClient = func(string, string, string) azureClientShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAZClient = oldNewAzureClient
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Address: &pb.Address{Name: "topicName"}}

	msg := pb.Message{}
	err = prov.DeadLetter(ctx, src, msg.GetUuid())
	assert.Contains(t, err.GetMessage(), "message not found in active messages")

	amock.AssertExpectations(t)
	nmsg.AssertExpectations(t)
}

func Test_Nack_NackError(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureClientMock{}
	amock.On("ReceiveMessages", mock.Anything,
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("int"),
		mock.AnythingOfType("chan azure.azureMessageShim"),
		mock.AnythingOfType("bool")).Return(nil)

	amock.Receives = make([]*azureMsgMock, 0)
	nmsg := &azureMsgMock{}
	props := make(map[string]interface{})
	props["header1"] = "value"
	nmsg.On("Properties").Return(props)
	nmsg.On("ContentType").Return("json")
	nmsg.On("Data").Return("hello")
	nmsg.On("Nack", mock.Anything, false).Return(errors.New("NackError"))

	amock.Receives = append(amock.Receives, nmsg)
	amock.On("CreateSubscription").Return(nil)

	rules := make([]azadmin.RuleProperties, 0)

	amock.On("ListRules").Return(rules, nil)
	amock.On("CreateTopic", ctx, "topicName").Return(nil)
	amock.On("Connect").Return(nil)

	oldNewAzureClient := NewAZClient

	NewAZClient = func(string, string, string) azureClientShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAZClient = oldNewAzureClient
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Address: &pb.Address{Name: "topicName"}}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc
	assert.NotNil(t, msg)
	time.Sleep(100 * time.Millisecond)
	err = prov.Nack(ctx, msg.GetUuid())
	assert.NotNil(t, err)
	assert.Equal(t, err.GetMessage(), "NackError")

	amock.AssertExpectations(t)
	nmsg.AssertExpectations(t)
}

func Test_Retry(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureClientMock{}
	amock.On("ReceiveMessages", mock.Anything,
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("int"),
		mock.AnythingOfType("chan azure.azureMessageShim"),
		mock.AnythingOfType("bool")).Return(nil)

	amock.Receives = make([]*azureMsgMock, 0)
	nmsg := &azureMsgMock{}
	props := make(map[string]interface{})
	props["header1"] = "value"
	nmsg.On("Properties").Return(props)
	nmsg.On("ContentType").Return("json")
	nmsg.On("Data").Return("hello")

	nmsg.On("ID").Return("1234")
	nmsg.On("DeliveryCount").Return(0)
	nmsg.On("Ack").Return(nil)
	nmsg.On("Schedule").Return(nil)

	amock.Receives = append(amock.Receives, nmsg)

	rules := make([]azadmin.RuleProperties, 0)
	amock.On("ListRules").Return(rules, nil)
	amock.On("Connect").Return(nil)
	amock.On("CreateTopic", ctx, "topicName").Return(nil)
	amock.On("CreateSubscription").Return(nil)

	senderMock := &azureSenderMock{}
	senderMock.On("Close").Return(nil)
	amock.On("NewSender").Return(senderMock, nil)

	oldNewAzureClient := NewAZClient

	NewAZClient = func(string, string, string) azureClientShim {
		return amock
	}

	oldNewAzureMsg := NewAzureMsgWithSender
	NewAzureMsgWithSender = func(azureSenderShim) azureMessageShim {
		return nmsg
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAZClient = oldNewAzureClient
		NewAzureMsgWithSender = oldNewAzureMsg
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Address: &pb.Address{Name: "topicName"}}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc
	time.Sleep(100 * time.Millisecond)
	err = prov.Retry(ctx, src, msg.GetUuid(), 1)
	assert.NotNil(t, msg)
	assert.Nil(t, err)

	amock.AssertExpectations(t)
	nmsg.AssertExpectations(t)
}

func Test_Retry_ScheduleFail(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureClientMock{}
	amock.On("ReceiveMessages", mock.Anything,
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("int"),
		mock.AnythingOfType("chan azure.azureMessageShim"),
		mock.AnythingOfType("bool")).Return(nil)

	amock.Receives = make([]*azureMsgMock, 0)
	nmsg := &azureMsgMock{}
	props := make(map[string]interface{})
	props["header1"] = "value"
	nmsg.On("Properties").Return(props)
	nmsg.On("ContentType").Return("json")
	nmsg.On("Data").Return("hello")
	nmsg.On("Nack", mock.Anything, true).Return(nil)
	nmsg.On("ID").Return("1234")
	nmsg.On("DeliveryCount").Return(0)
	nmsg.On("Schedule").Return(errors.New("Failed to schedule message"))

	amock.Receives = append(amock.Receives, nmsg)

	rules := make([]azadmin.RuleProperties, 0)
	amock.On("ListRules").Return(rules, nil)
	amock.On("Connect").Return(nil)
	amock.On("CreateTopic", ctx, "topicName").Return(nil)
	amock.On("CreateSubscription").Return(nil)

	senderMock := &azureSenderMock{}
	senderMock.On("Close").Return(nil)
	amock.On("NewSender").Return(senderMock, nil)

	oldNewAzureClient := NewAZClient

	NewAZClient = func(string, string, string) azureClientShim {
		return amock
	}

	oldNewAzureMsg := NewAzureMsgWithSender
	NewAzureMsgWithSender = func(azureSenderShim) azureMessageShim {
		return nmsg
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAZClient = oldNewAzureClient
		NewAzureMsgWithSender = oldNewAzureMsg
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Address: &pb.Address{Name: "topicName"}}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc
	time.Sleep(100 * time.Millisecond)
	err = prov.Retry(ctx, src, msg.GetUuid(), 1)
	assert.NotNil(t, msg)
	assert.Contains(t, err.GetMessage(), fmt.Sprintf("Failed to schedule retry message [%s], requeueing instead", msg.GetUuid()))

	amock.AssertExpectations(t)
	nmsg.AssertExpectations(t)
}

func Test_Retry_errorCreateTopic(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureClientMock{}
	amock.On("ReceiveMessages", mock.Anything,
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("int"),
		mock.AnythingOfType("chan azure.azureMessageShim"),
		mock.AnythingOfType("bool")).Return(nil)

	amock.Receives = make([]*azureMsgMock, 0)
	nmsg := &azureMsgMock{}
	props := make(map[string]interface{})
	props["header1"] = "value"
	nmsg.On("Properties").Return(props)
	nmsg.On("ContentType").Return("json")
	nmsg.On("Data").Return("hello")
	nmsg.On("Nack", mock.Anything, true).Return(nil)
	nmsg.On("ID").Return("1234")
	nmsg.On("DeliveryCount").Return(0)

	amock.Receives = append(amock.Receives, nmsg)

	rules := make([]azadmin.RuleProperties, 0)
	amock.On("ListRules").Return(rules, nil)
	amock.On("Connect").Return(nil)
	amock.On("CreateTopic", ctx, "topicName").Return(nil).Once()
	amock.On("CreateTopic", ctx, "topicName").Return(errors.New("createTopicErr")).Once()
	amock.On("CreateSubscription").Return(nil)

	senderMock := &azureSenderMock{}
	senderMock.On("Close").Return(nil)
	// amock.On("NewSender").Return(senderMock, nil)

	oldNewAzureClient := NewAZClient

	NewAZClient = func(string, string, string) azureClientShim {
		return amock
	}

	oldNewAzureMsg := NewAzureMsgWithSender
	NewAzureMsgWithSender = func(azureSenderShim) azureMessageShim {
		return nmsg
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAZClient = oldNewAzureClient
		NewAzureMsgWithSender = oldNewAzureMsg
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Address: &pb.Address{Name: "topicName"}}
	mc := make(chan *pb.Message)
	defer close(mc)
	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	msg := <-mc
	time.Sleep(100 * time.Millisecond)
	err = prov.Retry(ctx, src, msg.GetUuid(), 1)
	assert.NotNil(t, msg)
	assert.Contains(t, err.GetMessage(), fmt.Sprintf("Failed to publish retry message [%s]. Create topic failed. Requeueing instead", msg.GetUuid()))

	amock.AssertExpectations(t)
	nmsg.AssertExpectations(t)
}

func Test_Retry_NoMsgWithUUID(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureClientMock{}

	amock.Receives = make([]*azureMsgMock, 0)
	nmsg := &azureMsgMock{}
	props := make(map[string]interface{})
	props["header1"] = "value"

	amock.Receives = append(amock.Receives, nmsg)

	amock.On("Connect").Return(nil)

	senderMock := &azureSenderMock{}
	senderMock.On("Close").Return(nil)

	oldNewAzureClient := NewAZClient

	NewAZClient = func(string, string, string) azureClientShim {
		return amock
	}

	oldNewAzureMsg := NewAzureMsgWithSender
	NewAzureMsgWithSender = func(azureSenderShim) azureMessageShim {
		return nmsg
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAZClient = oldNewAzureClient
		NewAzureMsgWithSender = oldNewAzureMsg
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Address: &pb.Address{Name: "topicName"}}
	msgID := "12345"
	time.Sleep(100 * time.Millisecond)
	err = prov.Retry(ctx, src, msgID, 1)
	assert.Equal(t, "No message with uuid 12345", err.GetMessage())

	amock.AssertExpectations(t)
	nmsg.AssertExpectations(t)
}

func Test_Retry_CreateSenderFail(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureClientMock{}

	amock.Receives = make([]*azureMsgMock, 0)
	nmsg := &azureMsgMock{}
	props := make(map[string]interface{})
	props["header1"] = "value"

	amock.Receives = append(amock.Receives, nmsg)

	amock.On("Connect").Return(nil)

	senderMock := &azureSenderMock{}
	senderMock.On("Close").Return(nil)

	oldNewAzureClient := NewAZClient

	NewAZClient = func(string, string, string) azureClientShim {
		return amock
	}

	oldNewAzureMsg := NewAzureMsgWithSender
	NewAzureMsgWithSender = func(azureSenderShim) azureMessageShim {
		return nmsg
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAZClient = oldNewAzureClient
		NewAzureMsgWithSender = oldNewAzureMsg
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Address: &pb.Address{Name: "topicName"}}
	msgID := "12345"
	time.Sleep(100 * time.Millisecond)
	err = prov.Retry(ctx, src, msgID, 1)
	assert.Equal(t, "No message with uuid 12345", err.GetMessage())

	amock.AssertExpectations(t)
	nmsg.AssertExpectations(t)
}

func Test_Ack_NoConnect(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()
	msg := pb.Message{}
	err := prov.Ack(ctx, msg.GetUuid())
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "Could not retrieve client-id from context")
}

func Test_Nack_NoConnect(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()
	msg := pb.Message{}
	err := prov.Nack(ctx, msg.GetUuid())
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "Could not retrieve client-id from context")
}

func Test_Publish_NoConnect(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()
	mc := make(chan *pb.Message)
	ec := make(chan *pb.Error)
	err := prov.Publish(ctx, mc, ec)
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "Could not retrieve client-id from context")
}

func Test_Subscribe_NoConnect(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()
	address := &pb.Address{Name: "topicName"}
	src := &pb.Source{Address: address}
	mc := make(chan *pb.Message)

	err := prov.Subscribe(ctx, src, mc)
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "Could not retrieve client-id from context")
}

func Test_Subscribe_NotopicName(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()
	address := &pb.Address{}
	src := &pb.Source{Address: address}
	mc := make(chan *pb.Message)

	err := prov.Subscribe(ctx, src, mc)
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "address name not defined")
}

func Test_Subscribe_NoAddress(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()
	src := &pb.Source{}
	mc := make(chan *pb.Message)

	err := prov.Subscribe(ctx, src, mc)
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "address name not defined")
}

// Disconnect does not return anything so there isn't much to test
func Test_Disconnect(t *testing.T) {
	prov := NewAzureProvider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureClientMock{}
	amock.On("Connect").Return(nil)
	oldNewAzureClient := NewAZClient

	NewAZClient = func(string, string, string) azureClientShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAZClient = oldNewAzureClient
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)
	prov.Disconnect(ctx)

	amock.AssertExpectations(t)
}

func Test_SupportedSourceOptions(t *testing.T) {
	prov := NewAzureProvider()
	opts := prov.SupportedSourceOptions()
	assert.NotNil(t, opts)
	expected := make(map[string]bool)
	expected["MessageTTL"] = true
	expected["DeadLetterAddress"] = true
	expected["DeadLetterSubject"] = true
	expected["Expires"] = true

	assert.Equal(t, opts, expected)
}

func Test_WaitForConnect(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureClientMock{blockConnect: 1}
	amock.On("Connect").Return(nil)

	oldNewAzureClient := NewAZClient

	NewAZClient = func(string, string, string) azureClientShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAZClient = oldNewAzureClient
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)

	assert.Nil(t, err)
	connected := prov.WaitForConnect(ctx)

	assert.True(t, connected)

	amock.AssertExpectations(t)
}

func Test_Publish(t *testing.T) {
	ctx := context.Background()
	prov := NewAzureProvider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	subjects := make([]string, 0)
	subjects = append(subjects, "subject1")
	address := &pb.Address{Name: "topicName", Subjects: subjects, Type: pb.Address_FILTER}

	msg := &pb.Message{Address: address, Body: []byte("thebody")}
	msg.Headers = make(map[string]string)
	msg.Headers["Content-Type"] = "application/json"
	msg.Headers["Content-Encoding"] = "utf8"

	amock := &azureClientMock{}
	msgMock := &azureMsgMock{}
	senderMock := &azureSenderMock{}
	senderMock.On("Close").Return(nil)

	amock.On("NewSender").Return(senderMock, nil)
	msgMock.On("Send").Return(nil)
	msgMock.On("SetContentType").Return()

	amock.On("Connect").Return(nil)
	amock.On("CreateTopic", ctx, "topicName").Return(nil)
	oldNewAzureClient := NewAZClient

	NewAZClient = func(string, string, string) azureClientShim {
		return amock
	}

	oldNewAzureMsg := NewAzureMsgWithSender
	NewAzureMsgWithSender = func(azureSenderShim) azureMessageShim {
		return msgMock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAZClient = oldNewAzureClient
		NewAzureMsgWithSender = oldNewAzureMsg
	}()

	mc := make(chan *pb.Message)
	errchan := make(chan *pb.Error)

	go func() {
		mc <- msg
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	go func() {
		suberr := prov.Publish(ctx, mc, errchan)
		assert.Nil(t, suberr)
	}()

	time.Sleep(100 * time.Millisecond)

	amock.AssertExpectations(t)
}

func Test_Publish_Error(t *testing.T) {
	ctx := context.Background()
	prov := NewAzureProvider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	subjects := make([]string, 0)
	subjects = append(subjects, "subject1")
	address := &pb.Address{Name: "topicName", Subjects: subjects, Type: pb.Address_FILTER}

	msg := &pb.Message{Address: address, Body: []byte("thebody")}
	msg.Headers = make(map[string]string)
	msg.Headers["Content-Type"] = "application/json"
	msg.Headers["Content-Encoding"] = "utf8"

	amock := &azureClientMock{}
	msgMock := &azureMsgMock{}
	senderMock := &azureSenderMock{}
	senderMock.On("Close").Return(nil)
	msgMock.On("Send").Return(errors.New("an error occured"))

	amock.On("Connect").Return(nil)
	amock.On("CreateTopic", ctx, "topicName").Return(nil)

	amock.On("NewSender").Return(senderMock, nil)

	oldNewAzureClient := NewAZClient

	NewAZClient = func(string, string, string) azureClientShim {
		return amock
	}

	oldNewAzureMsg := NewAzureMsgWithSender
	NewAzureMsgWithSender = func(azureSenderShim) azureMessageShim {
		return msgMock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAZClient = oldNewAzureClient
		NewAzureMsgWithSender = oldNewAzureMsg
	}()

	mc := make(chan *pb.Message)
	errchan := make(chan *pb.Error)

	go func() {
		mc <- msg
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	go func() {
		suberr := prov.Publish(ctx, mc, errchan)
		assert.Nil(t, suberr)
	}()

	time.Sleep(100 * time.Millisecond)

	pbErr := <-errchan
	assert.NotNil(t, pbErr)
	assert.Equal(t, pbErr.GetMessage(), "an error occured")

	amock.AssertExpectations(t)
}

func Test_durationTo8601(t *testing.T) {
	d := time.Duration(5000 * time.Millisecond)
	s := durationTo8601(d)
	assert.Equal(t, "PT0M5S", s)

	d = time.Duration(300000 * time.Millisecond)
	s = durationTo8601(d)
	assert.Equal(t, "PT5M0S", s)
}

func Test_generateRules_with_filters(t *testing.T) {

	// prov := NewAzureProvider()

	options := make(map[string]string)
	options["MessageTTL"] = "100"
	options["Expires"] = "100"
	options["DeadLetterAddress"] = "dla"
	options["DeadLetterSubject"] = "dls"

	subjects := make([]string, 0)
	subjects = append(subjects, "subject1")
	subjects = append(subjects, "subject2")
	parent := &pb.Address{Name: "parent", Type: pb.Address_QUEUE}
	address := &pb.Address{Name: "topicName", Subjects: subjects, ParentAddress: parent, Type: pb.Address_FILTER}
	matches1 := make([]*pb.Match, 0)
	matches1 = append(matches1, &pb.Match{Name: "key1", Value: "value1"})
	matches1 = append(matches1, &pb.Match{Name: "key2", Value: "value2"})
	matches2 := make([]*pb.Match, 0)
	matches2 = append(matches2, &pb.Match{Name: "key3", Value: "value3"})
	matches2 = append(matches2, &pb.Match{Name: "key4", Value: "value4"})
	filters := make([]*pb.Filter, 0)
	filters = append(filters, &pb.Filter{Matches: matches1, Type: pb.Filter_ANY})
	filters = append(filters, &pb.Filter{Matches: matches2, Type: pb.Filter_ANY})

	src := &pb.Source{Name: "srcname",
		Address:       address,
		Options:       options,
		Filters:       filters,
		Exclusive:     true,
		AutoDelete:    true,
		PrefetchCount: 4}

	rules := generateRules(src)

	fmt.Printf("%s", rules)

	assert.Equal(t, 2, len(rules))
	assert.Equal(t, "RoutingKey = 'subject1' AND ((\"key1\" = 'value1' OR \"key2\" = 'value2') OR (\"key3\" = 'value3' OR \"key4\" = 'value4'))", rules[0])
	assert.Equal(t, "RoutingKey = 'subject2' AND ((\"key1\" = 'value1' OR \"key2\" = 'value2') OR (\"key3\" = 'value3' OR \"key4\" = 'value4'))", rules[1])
}

func Test_generateRules_no_filters(t *testing.T) {

	subjects := make([]string, 0)
	subjects = append(subjects, "subject1")
	subjects = append(subjects, "subject2")
	parent := &pb.Address{Name: "parent", Type: pb.Address_QUEUE}
	address := &pb.Address{Name: "topicName", Subjects: subjects, ParentAddress: parent, Type: pb.Address_FILTER}
	src := &pb.Source{Name: "srcname",
		Address:       address,
		Exclusive:     true,
		AutoDelete:    true,
		PrefetchCount: 4}

	rules := generateRules(src)

	fmt.Printf("%s", rules)

	assert.Equal(t, 2, len(rules))
	assert.Equal(t, "RoutingKey = 'subject1'", rules[0])
	assert.Equal(t, "RoutingKey = 'subject2'", rules[1])
}

func Test_Subscribe_Options(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()

	options := make(map[string]string)
	options["MessageTTL"] = "100"
	options["Expires"] = "100"
	options["DeadLetterAddress"] = "dla"
	options["DeadLetterSubject"] = "dls"

	expectedQueueArgs := make(map[string]interface{})
	expectedQueueArgs["x-message-ttl"] = 100
	expectedQueueArgs["x-expires"] = 100
	expectedQueueArgs["x-dead-letter-exchange"] = "dla"
	expectedQueueArgs["x-dead-letter-routing-key"] = "dls"

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	subjects := make([]string, 0)
	subjects = append(subjects, "subject1")
	subjects = append(subjects, "subject2")
	parent := &pb.Address{Name: "parent", Type: pb.Address_QUEUE}
	address := &pb.Address{Name: "topicName", Subjects: subjects, ParentAddress: parent, Type: pb.Address_FILTER}
	matches1 := make([]*pb.Match, 0)
	matches1 = append(matches1, &pb.Match{Name: "key1", Value: "value1"})
	matches1 = append(matches1, &pb.Match{Name: "key2", Value: "value2"})
	matches2 := make([]*pb.Match, 0)
	matches2 = append(matches2, &pb.Match{Name: "key3", Value: "value3"})
	matches2 = append(matches2, &pb.Match{Name: "key4", Value: "value4"})
	filters := make([]*pb.Filter, 0)
	filters = append(filters, &pb.Filter{Matches: matches1, Type: pb.Filter_ANY})
	filters = append(filters, &pb.Filter{Matches: matches2, Type: pb.Filter_ANY})

	src := &pb.Source{Name: "srcname",
		Address:       address,
		Options:       options,
		Filters:       filters,
		Exclusive:     true,
		AutoDelete:    true,
		PrefetchCount: 4}

	expectedMatchHeaders1 := make(map[string]string)
	expectedMatchHeaders1["key1"] = "value1"
	expectedMatchHeaders1["key2"] = "value2"
	expectedMatchHeaders1["x-match"] = "any"

	expectedMatchHeaders2 := make(map[string]string)
	expectedMatchHeaders2["key3"] = "value3"
	expectedMatchHeaders2["key4"] = "value4"
	expectedMatchHeaders2["x-match"] = "any"

	amock := &azureClientMock{}
	msgMock := &azureMsgMock{}

	amock.On("ReceiveMessages", mock.Anything,
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		mock.AnythingOfType("int"),
		mock.AnythingOfType("chan azure.azureMessageShim"),
		mock.AnythingOfType("bool")).Return(nil)

	amock.Receives = make([]*azureMsgMock, 0)
	nmsg := &azureMsgMock{}
	props := make(map[string]interface{})
	props["header1"] = "value"
	nmsg.On("Properties").Return(props)
	nmsg.On("ContentType").Return("json")
	nmsg.On("Data").Return("hello")
	nmsg.On("Ack").Return(nil)

	amock.Receives = append(amock.Receives, nmsg)

	amock.On("CreateSubscription").Return(nil)
	amock.On("GenerateForwardToName").Return("sb://namespace/topic").Once()
	amock.On("GenerateForwardToName").Return(fmt.Sprintf("sb://namespace/%s", expectedQueueArgs["x-dead-letter-exchange"])).Once()

	// smMock.On("Create").Return(nil)
	// smMockParent.On("Create").Return(nil)
	rules := make([]azadmin.RuleProperties, 0)
	amock.On("ListRules").Return(rules, nil)
	// smMockParent.On("ListRules").Return(rules, nil)

	// dla
	amock.On("CreateRule",
		ctx,
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		"RoutingAndFilterRule00",
		"RoutingKey = 'subject1'",
	).Return(nil).Once()

	amock.On("CreateRule",
		ctx,
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		"RoutingAndFilterRule01",
		"RoutingKey = 'subject2'",
	).Return(nil).Once()

	// topic
	amock.On("CreateRule",
		ctx,
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		"RoutingAndFilterRule00",
		`RoutingKey = 'subject1' AND (("key1" = 'value1' OR "key2" = 'value2') OR ("key3" = 'value3' OR "key4" = 'value4'))`,
	).Return(nil).Once()

	amock.On("CreateRule",
		ctx,
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		"RoutingAndFilterRule01",
		`RoutingKey = 'subject2' AND (("key1" = 'value1' OR "key2" = 'value2') OR ("key3" = 'value3' OR "key4" = 'value4'))`,
	).Return(nil).Once()

	// dlq rule should be same as for original subscription
	amock.On("CreateRule",
		ctx,
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		"RoutingAndFilterRule00",
		"RoutingKey = 'subject1'",
	).Return(nil).Once()
	amock.On("CreateRule",
		ctx,
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
		"RoutingAndFilterRule01",
		"RoutingKey = 'subject2'",
	).Return(nil).Once()

	amock.On("Connect").Return(nil)
	amock.On("CreateTopic", ctx, "dla").Return(nil)
	amock.On("CreateTopic", ctx, "topicName").Return(nil)
	amock.On("CreateTopic", ctx, "parent").Return(nil)

	oldNewAzureClient := NewAZClient

	NewAZClient = func(string, string, string) azureClientShim {
		return amock
	}

	oldNewAzureMsg := NewAzureMsgWithSender
	NewAzureMsgWithSender = func(azureSenderShim) azureMessageShim {
		return msgMock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAZClient = oldNewAzureClient
		NewAzureMsgWithSender = oldNewAzureMsg
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)
	var msg *pb.Message

	mc := make(chan *pb.Message)
	defer close(mc)

	go func() {
		suberr := prov.Subscribe(ctx, src, mc)
		assert.Nil(t, suberr)
	}()

	go func() {
		msg = <-mc
	}()

	time.Sleep(100 * time.Millisecond)
	err = prov.Ack(ctx, msg.GetUuid())
	assert.NotNil(t, msg)
	assert.Nil(t, err)
	assert.NotNil(t, msg.GetAddress())
	assert.Equal(t, msg.GetAddress(), src.GetAddress())
	assert.Equal(t, msg.GetAddress().GetSubjects(), subjects)

	amock.AssertExpectations(t)
	nmsg.AssertExpectations(t)
}

func Test_declareSubscriptionWithOptions(t *testing.T) {
	ctx := context.Background()
	subName := "subName"
	topicName := "topicName"
	amock := &azureClientMock{}
	amock.On("CreateSubscription").Return(nil)

	rules := make([]azadmin.RuleProperties, 0)
	defaultRule := azadmin.RuleProperties{
		Name: "$Default",
	}
	rules = append(rules, defaultRule)
	ruleName := "RoutingAndFilterRule00"
	routingRule := azadmin.RuleProperties{
		Name:   ruleName,
		Filter: &azadmin.SQLFilter{},
		Action: &azadmin.SQLAction{},
	}
	rules = append(rules, routingRule)

	amock.On("ListRules").Return(rules, nil)
	amock.On("DeleteRule", mock.Anything, "topicName", sourceNameToSubName("subName"), "$Default").Return(nil).Once()

	actualRule := "RoutingKey like 'subject%hash%star'"

	amock.On("UpdateRule", ctx, topicName, sourceNameToSubName(subName), ruleName, actualRule).Return(nil).Once()

	bd := &BrokerDetails{}
	bd.azure = amock
	bd.knownTopics = util.NewConcurrentMap()
	address := &pb.Address{Type: 1}
	subjects := []string{"subject#hash*star"}
	address.Subjects = subjects
	source := &pb.Source{}
	source.Name = subName
	source.Address = address
	subOpts := &azadmin.CreateSubscriptionOptions{}
	_, err := declareSubscriptionWithOptions(source, bd, topicName, subOpts)
	assert.Nil(t, err)
	amock.AssertExpectations(t)
}

func Test_declareSubscriptionWithOptions_removeExtra(t *testing.T) {
	subName := "subName"
	topicName := "topicName"
	amock := &azureClientMock{}
	amock.On("CreateSubscription").Return(nil)

	actualRule := "RoutingKey like 'subject%hash%star'"

	rules := make([]azadmin.RuleProperties, 0)
	defaultRule := azadmin.RuleProperties{
		Name: "$Default",
	}
	rules = append(rules, defaultRule)
	rules = append(rules, azadmin.RuleProperties{
		Name: "RoutingAndFilterRule00",
		Filter: &azadmin.SQLFilter{
			Expression: actualRule,
		},
		Action: &azadmin.SQLAction{},
	})
	rules = append(rules, azadmin.RuleProperties{
		Name: "RoutingAndFilterRule01",
		Filter: &azadmin.SQLFilter{
			Expression: actualRule,
		},
		Action: &azadmin.SQLAction{},
	})

	amock.On("ListRules").Return(rules, nil)
	amock.On("DeleteRule", mock.Anything, topicName, sourceNameToSubName(subName), "$Default").Return(nil).Once()

	// amock.On("CreateRule", ctx, topicName, sourceNameToSubName(subName), "RoutingAndFilterRule00", actualRule).Return(nil).Once()
	amock.On("DeleteRule", mock.Anything, topicName, sourceNameToSubName(subName), "RoutingAndFilterRule01").Return(nil).Once()

	bd := &BrokerDetails{}
	bd.azure = amock
	bd.knownTopics = util.NewConcurrentMap()
	address := &pb.Address{Type: 1}
	subjects := []string{"subject#hash*star"}
	address.Subjects = subjects
	source := &pb.Source{}
	source.Name = subName
	source.Address = address
	subOpts := &azadmin.CreateSubscriptionOptions{}
	_, err := declareSubscriptionWithOptions(source, bd, topicName, subOpts)
	assert.Nil(t, err)

	amock.AssertExpectations(t)
}

func Test_declareSubscriptionWithOptions_ruleDoesNotMatch(t *testing.T) {
	ctx := context.Background()
	subName := "subName"
	topicName := "topicName"
	amock := &azureClientMock{}
	amock.On("CreateSubscription").Return(nil)

	actualRule := "RoutingKey like 'subject%hash%star'"

	rules := make([]azadmin.RuleProperties, 0)
	defaultRule := azadmin.RuleProperties{
		Name: "$Default",
	}
	rules = append(rules, defaultRule)
	rules = append(rules, azadmin.RuleProperties{
		Name: "RoutingAndFilterRule00",
		Filter: &azadmin.SQLFilter{
			Expression: "not the correct rule text",
		},
		Action: &azadmin.SQLAction{},
	})
	amock.On("ListRules").Return(rules, nil)
	amock.On("DeleteRule", mock.Anything, topicName, sourceNameToSubName(subName), "$Default").Return(nil).Once()

	amock.On("UpdateRule", ctx, topicName, sourceNameToSubName(subName), "RoutingAndFilterRule00", actualRule).Return(nil).Once()

	bd := &BrokerDetails{}
	bd.azure = amock
	bd.knownTopics = util.NewConcurrentMap()
	address := &pb.Address{Type: 1}
	subjects := []string{"subject#hash*star"}
	address.Subjects = subjects
	source := &pb.Source{}
	source.Name = subName
	source.Address = address
	subOpts := &azadmin.CreateSubscriptionOptions{}
	_, err := declareSubscriptionWithOptions(source, bd, topicName, subOpts)
	assert.Nil(t, err)

	amock.AssertExpectations(t)
}

func Test_declareSubscriptionWithOptions_CreateSubscriptionError(t *testing.T) {

	subName := "subName"
	topicName := "topicName"

	amock := &azureClientMock{}
	amock.On("CreateTopic").Return(nil)
	err := errors.New("error")
	amock.On("CreateSubscription").Return(err)

	bd := &BrokerDetails{}
	bd.azure = amock
	bd.knownTopics = util.NewConcurrentMap()
	address := &pb.Address{Type: 1}
	subjects := []string{"subject#hash*star"}
	address.Subjects = subjects
	source := &pb.Source{}
	source.Name = subName
	source.Address = address
	subOpts := &azadmin.CreateSubscriptionOptions{}

	_, cserr := declareSubscriptionWithOptions(source, bd, topicName, subOpts)
	assert.Equal(t, err, cserr)
}

func Test_declareSubscriptionWithOptions_ListRulesError(t *testing.T) {
	ctx := context.Background()

	subName := "subName"
	topicName := "topicName"

	amock := &azureClientMock{}
	amock.On("CreateTopic").Return(nil)
	err := errors.New("error")
	amock.On("CreateSubscription").Return(nil)

	rules := make([]azadmin.RuleProperties, 0)
	amock.On("ListRules").Return(rules, err)

	amock.On("DeleteRule", mock.Anything, "topicName", sourceNameToSubName("subName"), "$Default").Return(nil).Once()

	ruleName := "RoutingAndFilterRule00"
	actualRule := "RoutingKey like 'subject%hash%star'"

	amock.On("CreateRule", ctx, topicName, sourceNameToSubName(subName), ruleName, actualRule).Return(nil).Once()

	bd := &BrokerDetails{}
	bd.azure = amock
	bd.knownTopics = util.NewConcurrentMap()
	address := &pb.Address{Type: 1}
	subjects := []string{"subject#hash*star"}
	address.Subjects = subjects
	source := &pb.Source{}
	source.Name = subName
	source.Address = address
	subOpts := &azadmin.CreateSubscriptionOptions{}
	_, err = declareSubscriptionWithOptions(source, bd, topicName, subOpts)
	assert.Nil(t, err)
}

func Test_declareExchange_errorAddressType(t *testing.T) {
	bd := &BrokerDetails{}
	address := &pb.Address{Type: 4}
	_, err := declareExchange(address, bd)
	assert.Error(t, err)
	assert.Equal(t, "4 is not a valid address type", err.Error())

}

func Test_sourceNameToSubName_LongName(t *testing.T) {
	srcName := "ReallyLongNameOverTwentyFiveCharacters"
	subName := sourceNameToSubName(srcName)
	idx := strings.Index(subName, "-")
	assert.Equal(t, idx, 25)
}

func Test_ClientExists(t *testing.T) {
	prov := NewAzureProvider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureClientMock{}
	amock.On("Connect").Return(nil)
	oldNewAzureClient := NewAZClient

	NewAZClient = func(string, string, string) azureClientShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAZClient = oldNewAzureClient
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	exists := prov.ClientExists("1234")
	assert.True(t, exists)
}

func Test_ClientExists_false(t *testing.T) {
	prov := NewAzureProvider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureClientMock{}
	amock.On("Connect").Return(nil)
	oldNewAzureClient := NewAZClient

	NewAZClient = func(string, string, string) azureClientShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAZClient = oldNewAzureClient
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(ctx, cc, false)
	assert.Nil(t, err)

	exists := prov.ClientExists("4321")
	assert.False(t, exists)
}

func Test_sourceNameToSubName_ShortName(t *testing.T) {
	srcName := "ShortName"
	subName := sourceNameToSubName(srcName)
	idx := strings.Index(subName, "-")
	assert.Equal(t, idx, 9)
}

func Test_ClientSentTime(t *testing.T) {
	am := azureMessage{}
	now := time.Now()
	am.clientSentTime = now
	assert.Equal(t, now, am.ClientSentTime())
}

func Test_SetClientSentTime(t *testing.T) {
	am := azureMessage{}
	am.SetClientSentTime()
	assert.Equal(t, am.clientSentTime, am.ClientSentTime())
}

func Test_SetContentType_ReceivedMessage(t *testing.T) {
	am := azureMessage{}
	am.receivedMessage = &azservicebus.ReceivedMessage{}
	am.SetContentType("text/plain")
	assert.Equal(t, "text/plain", am.ContentType())
}

func Test_SetContentType_SendingMessage(t *testing.T) {
	am := azureMessage{}
	am.sendingMessage = &azservicebus.Message{}
	am.SetContentType("text/plain")
	assert.Equal(t, "text/plain", am.ContentType())
}

func Test_DeliveryCount(t *testing.T) {
	am := azureMessage{}
	am.receivedMessage = &azservicebus.ReceivedMessage{DeliveryCount: 10}
	assert.Equal(t, uint32(10), am.DeliveryCount())
}

func Test_Data_ReceivedMessage(t *testing.T) {
	am := azureMessage{}
	am.receivedMessage = &azservicebus.ReceivedMessage{}
	am.SetData([]byte("data"))
	// can't set data on a received message
	assert.Nil(t, am.Data())
}

func Test_Data_SendingMessage(t *testing.T) {

	am := azureMessage{}
	am.sendingMessage = &azservicebus.Message{}
	am.SetData([]byte("data"))
	assert.Equal(t, []byte("data"), am.Data())
}

func Test_ID_ReceivedMessage(t *testing.T) {
	am := azureMessage{}
	am.receivedMessage = &azservicebus.ReceivedMessage{MessageID: "myID"}
	assert.Equal(t, "myID", am.ID())
}

func Test_ID_SendingMessage(t *testing.T) {
	am := azureMessage{}
	id := "myID"
	am.sendingMessage = &azservicebus.Message{MessageID: &id}
	assert.Equal(t, "myID", am.ID())
}

func Test_propertiesToHeaders(t *testing.T) {
	p := make(map[string]interface{})
	p["int"] = 1
	p["string"] = "s"

	h := propertiesToHeaders(p)
	assert.Equal(t, "1", h["int"])
	assert.Equal(t, "s", h["string"])
}

func Test_GenerateForwardName(t *testing.T) {
	ac := NewAzureClient("hostname", "username", "password")
	fn := ac.GenerateForwardToName("topicName")
	assert.Equal(t, "sb://hostname/topicName", fn)
}

func Test_LockedUntil(t *testing.T) {
	lockTime := time.Now()
	am := azureMessage{}
	am.receivedMessage = &azservicebus.ReceivedMessage{LockedUntil: &lockTime}
	assert.Equal(t, lockTime, am.LockedUntil())
}

func Test_SetProperties_ReceivedMessage(t *testing.T) {
	am := azureMessage{}
	am.receivedMessage = &azservicebus.ReceivedMessage{}

	p := make(map[string]interface{})
	p["key"] = "value"
	am.SetProperties(p)
	prop, ok := am.Property("key")
	assert.True(t, ok)
	assert.Equal(t, "value", prop)
}

func Test_SetProperty_ReceivedMessage(t *testing.T) {
	am := azureMessage{}
	am.receivedMessage = &azservicebus.ReceivedMessage{ApplicationProperties: make(map[string]interface{})}
	am.SetProperty("key", "value")
	prop, ok := am.Property("key")
	assert.True(t, ok)
	assert.Equal(t, "value", prop)
}

func Test_SetProperties_SendingMessage(t *testing.T) {
	am := azureMessage{}
	am.sendingMessage = &azservicebus.Message{}

	p := make(map[string]interface{})
	p["key"] = "value"
	am.SetProperties(p)
	prop, ok := am.Property("key")
	assert.True(t, ok)
	assert.Equal(t, "value", prop)
}

func Test_SetProperty_SendingMessage(t *testing.T) {
	am := azureMessage{}
	am.sendingMessage = &azservicebus.Message{ApplicationProperties: make(map[string]interface{})}
	am.SetProperty("key", "value")
	prop, ok := am.Property("key")
	assert.True(t, ok)
	assert.Equal(t, "value", prop)
}

func Test_Properties_ReceivedMessage(t *testing.T) {
	am := azureMessage{}
	am.receivedMessage = &azservicebus.ReceivedMessage{ApplicationProperties: make(map[string]interface{})}
	am.SetProperty("key", "value")
	props := am.Properties()
	assert.Equal(t, "value", props["key"])
}

func Test_Properties_SendingMessage(t *testing.T) {
	am := azureMessage{}
	am.sendingMessage = &azservicebus.Message{ApplicationProperties: make(map[string]interface{})}
	am.SetProperty("key", "value")
	props := am.Properties()
	assert.Equal(t, "value", props["key"])
}
