package azure

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-amqp-common-go/v3/uuid"
	servicebus "github.com/Azure/azure-service-bus-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	pb "sassoftware.io/convoy/arke/api"
)

func init() {
}

type azureTopicMock struct {
	mock.Mock
	AzureTopicShim
}

type azureNSMock struct {
	mock.Mock
	AzureNamespaceShim
	blockConnect time.Duration
}

type azureSMMock struct {
	mock.Mock
	AzureSubscriptionManagerShim
}

type MockRecv struct {
	Message *azureMsgMock
	Error   error
}

type azureSubMock struct {
	mock.Mock
	AzureSubscriptionShim
	Receives []*azureMsgMock
}

type azureMsgMock struct {
	mock.Mock
	AzureMessageShim
	properties map[string]interface{}
}

func (m *azureNSMock) NewTopic(name string) (AzureTopicShim, error) {
	args := m.Called(name)
	t := args.Get(0).(AzureTopicShim)
	return t, args.Error(1)
}
func (m *azureNSMock) Connect() error {
	args := m.Called()
	return args.Error(0)
}

func (m *azureNSMock) NewSubscriptionManager(topicName string) (AzureSubscriptionManagerShim, error) {
	args := m.Called()
	t := args.Get(0).(AzureSubscriptionManagerShim)
	return t, args.Error(1)
}

func (m *azureTopicMock) ScheduleAt(time.Time, ...AzureMessageShim) ([]int64, error) {
	args := m.Called()
	return nil, args.Error(0)
}

func (m *azureTopicMock) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *azureTopicMock) GetEntity() *servicebus.TopicEntity {
	args := m.Called()
	return args.Get(0).(*servicebus.TopicEntity)
}

func (m *azureTopicMock) GetName() string {
	args := m.Called()
	return args.String(0)
}

func (m *azureTopicMock) Send(context.Context, AzureMessageShim, ...servicebus.SendOption) error {
	args := m.Called()
	return args.Error(0)
}

func (m *azureTopicMock) NewSubscription(name string, opts ...servicebus.SubscriptionOption) (AzureSubscriptionShim, error) {
	args := m.Called()
	as := args.Get(0).(AzureSubscriptionShim)
	return as, args.Error(1)
}

func (m *azureSMMock) Create(string, ...servicebus.SubscriptionManagementOption) error {
	args := m.Called()
	return args.Error(0)
}

func (m *azureSMMock) ListRules(string) ([]*servicebus.RuleEntity, error) {
	args := m.Called()
	re := args.Get(0).([]*servicebus.RuleEntity)
	return re, args.Error(1)
}

func (m *azureSMMock) DeleteRule(string, string) error {
	args := m.Called()
	return args.Error(0)
}

func (m *azureSMMock) PutRule(subName string, ruleName string, ruleText string) (*servicebus.RuleEntity, error) {
	args := m.Called(subName, ruleName, ruleText)
	re := args.Get(0).(*servicebus.RuleEntity)
	return re, args.Error(1)
}

func (m *azureSubMock) Receive(ctx context.Context, messageChannel chan AzureMessageShim) error {
	args := m.Called(ctx, messageChannel)

	for _, msg := range m.Receives {
		messageChannel <- msg
	}
	return args.Error(0)
}

func (m *azureSubMock) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *azureSubMock) Name() string {
	args := m.Called()
	return args.String(0)
}

func (m *azureMsgMock) GetDeliveryCount() uint32 {
	args := m.Called()
	return uint32(args.Int(0))
}

func (m *azureMsgMock) GetID() string {
	args := m.Called()
	return args.String(0)
}

func (m *azureMsgMock) GetContentType() string {
	args := m.Called()
	return args.String(0)
}

func (m *azureMsgMock) GetData() []byte {

	args := m.Called()
	return []byte(fmt.Sprintf("%s", args.Get(0)))
}

func (m *azureMsgMock) SetLockToken(*uuid.UUID) {}

func (m *azureMsgMock) SetData(data []byte) {}

func (m *azureMsgMock) SetUserProperties(properties map[string]interface{}) {}
func (m *azureMsgMock) SetUserProperty(string, interface{})                 {}
func (m *azureMsgMock) SetContentType(string)                               {}

func (m *azureMsgMock) GetUserProperties() map[string]interface{} {
	args := m.Called()
	return args.Get(0).(map[string]interface{})
}

func (m *azureMsgMock) GetUserProperty(key string) (interface{}, bool) {
	val, ok := m.properties[key]
	return val, ok
}

func (m *azureMsgMock) Complete() error {
	args := m.Called()
	return args.Error(0)
}

func (m *azureMsgMock) Abandon() error {
	args := m.Called()
	return args.Error(0)
}

func TestNewAzureProvider(t *testing.T) {
	prov := NewAzureProvider()
	assert.NotNil(t, prov)
}

func TestConnect(t *testing.T) {
	prov := NewAzureProvider()
	cc := &pb.ConnectionConfiguration{}
	ctx := context.Background()
	err := prov.Connect(&ctx, cc, false)
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
	err := prov.Connect(&ctx, cc, false)

	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "noclient")
}

func TestConnect_Stats(t *testing.T) {
	prov := NewAzureProvider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureNSMock{}
	amock.On("Connect").Return(nil)
	oldNewAzureNS := NewAzureNS

	NewAzureNS = func(string) AzureNamespaceShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAzureNS = oldNewAzureNS
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
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

	amock := &azureNSMock{}
	amock.On("Connect").Return(nil)

	oldNewAzureNS := NewAzureNS

	NewAzureNS = func(string) AzureNamespaceShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAzureNS = oldNewAzureNS
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)
	msg := pb.Message{}
	err = prov.Ack(&ctx, msg.GetUuid())
	assert.Contains(t, err.GetMessage(), "No message with uuid")

	amock.AssertExpectations(t)
}

func Test_Nack_NoMsg(t *testing.T) {
	prov := NewAzureProvider()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureNSMock{}
	amock.On("Connect").Return(nil)

	oldNewAzureNS := NewAzureNS

	NewAzureNS = func(string) AzureNamespaceShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAzureNS = oldNewAzureNS
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)
	msg := pb.Message{}
	err = prov.Nack(&ctx, msg.GetUuid())
	assert.Contains(t, err.GetMessage(), "No message with uuid")

	amock.AssertExpectations(t)
}

func Test_Ack(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureNSMock{}
	tmock := &azureTopicMock{}
	mmock := &azureSMMock{}
	subMock := &azureSubMock{}
	tmock.On("GetName").Return("topicName")
	tmock.On("Close").Return(nil)
	subMock.On("Receive", ctx, mock.Anything).Return(nil)
	subMock.On("Close").Return(nil)
	subMock.On("Name").Return("sub")

	subMock.Receives = make([]*azureMsgMock, 0)
	nmsg := &azureMsgMock{}
	props := make(map[string]interface{})
	props["header1"] = "value"
	nmsg.On("SetData").Return(nil)
	nmsg.On("GetUserProperties").Return(props)
	nmsg.On("GetContentType").Return("json")
	nmsg.On("GetData").Return("hello")
	nmsg.On("Complete").Return(nil)

	subMock.Receives = append(subMock.Receives, nmsg)
	tmock.On("NewSubscription").Return(subMock, nil)

	mmock.On("Create").Return(nil)
	rules := make([]*servicebus.RuleEntity, 0)
	defaultRule := &servicebus.RuleEntity{Entity: &servicebus.Entity{
		Name: "$Default",
	}}
	rules = append(rules, defaultRule)
	mmock.On("ListRules").Return(rules, nil)
	// rule := &servicebus.RuleEntity{}
	mmock.On("DeleteRule").Return(nil)
	// mmock.On("PutRule").Return(rule, nil)

	mmock.On("PutRule",
		mock.AnythingOfType("string"),
		"RoutingAndFilterRule",
		"(user.RoutingKey = 'one' OR user.RoutingKey = 'two')",
	).Return(&servicebus.RuleEntity{}, nil).Once()

	amock.On("Connect").Return(nil)
	amock.On("NewTopic", "topicName").Return(tmock, nil).Once()
	amock.On("NewSubscriptionManager").Return(mmock, nil)

	oldNewAzureNS := NewAzureNS

	NewAzureNS = func(string) AzureNamespaceShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAzureNS = oldNewAzureNS
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Address: &pb.Address{Name: "topicName", Subjects: []string{"one", "two"}}}
	mc := make(chan *pb.Message)
	defer close(mc)
	stop := make(chan bool)
	go func() {
		suberr := prov.Subscribe(&ctx, src, mc, stop)
		assert.Nil(t, suberr)
	}()

	msg := <-mc
	time.Sleep(100 * time.Millisecond)
	err = prov.Ack(&ctx, msg.GetUuid())
	assert.NotNil(t, msg)
	assert.Nil(t, err)

	subMock.AssertExpectations(t)
	tmock.AssertExpectations(t)
	mmock.AssertExpectations(t)
	amock.AssertExpectations(t)
}

func Test_Ack_CompleteErr(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureNSMock{}
	tmock := &azureTopicMock{}
	mmock := &azureSMMock{}
	subMock := &azureSubMock{}
	tmock.On("GetName").Return("topicName")
	tmock.On("Close").Return(nil)
	subMock.On("Receive", ctx, mock.Anything).Return(nil)
	subMock.On("Close").Return(nil)
	subMock.On("Name").Return("sub")

	subMock.Receives = make([]*azureMsgMock, 0)
	nmsg := &azureMsgMock{}
	props := make(map[string]interface{})
	props["header1"] = "value"
	nmsg.On("SetData").Return(nil)
	nmsg.On("GetUserProperties").Return(props)
	nmsg.On("GetContentType").Return("json")
	nmsg.On("GetData").Return("hello")
	nmsg.On("Complete").Return(errors.New("AckError"))

	subMock.Receives = append(subMock.Receives, nmsg)
	tmock.On("NewSubscription").Return(subMock, nil)

	mmock.On("Create").Return(nil)
	rules := make([]*servicebus.RuleEntity, 0)
	defaultRule := &servicebus.RuleEntity{Entity: &servicebus.Entity{
		Name: "$Default",
	}}
	rules = append(rules, defaultRule)
	mmock.On("ListRules").Return(rules, nil)
	// rule := &servicebus.RuleEntity{}
	mmock.On("DeleteRule").Return(nil)
	// mmock.On("PutRule").Return(rule, nil)

	mmock.On("PutRule",
		mock.AnythingOfType("string"),
		"RoutingAndFilterRule",
		"(user.RoutingKey = 'one' OR user.RoutingKey = 'two')",
	).Return(&servicebus.RuleEntity{}, nil).Once()

	amock.On("Connect").Return(nil)
	amock.On("NewTopic", "topicName").Return(tmock, nil).Once()
	amock.On("NewSubscriptionManager").Return(mmock, nil)

	oldNewAzureNS := NewAzureNS

	NewAzureNS = func(string) AzureNamespaceShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAzureNS = oldNewAzureNS
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Address: &pb.Address{Name: "topicName", Subjects: []string{"one", "two"}}}
	mc := make(chan *pb.Message)
	defer close(mc)
	stop := make(chan bool)
	go func() {
		suberr := prov.Subscribe(&ctx, src, mc, stop)
		assert.Nil(t, suberr)
	}()

	msg := <-mc
	assert.NotNil(t, msg)
	time.Sleep(100 * time.Millisecond)
	err = prov.Ack(&ctx, msg.GetUuid())
	assert.NotNil(t, err)
	assert.Equal(t, err.GetMessage(), "AckError")

	subMock.AssertExpectations(t)
	tmock.AssertExpectations(t)
	mmock.AssertExpectations(t)
	amock.AssertExpectations(t)
}

func Test_Nack(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureNSMock{}
	tmock := &azureTopicMock{}
	mmock := &azureSMMock{}
	subMock := &azureSubMock{}
	tmock.On("GetName").Return("topicName")
	tmock.On("Close").Return(nil)
	subMock.On("Receive", ctx, mock.Anything).Return(nil)
	subMock.On("Close").Return(nil)
	subMock.On("Name").Return("sub")

	subMock.Receives = make([]*azureMsgMock, 0)
	nmsg := &azureMsgMock{}
	props := make(map[string]interface{})
	props["header1"] = "value"
	nmsg.On("SetData").Return(nil)
	nmsg.On("GetUserProperties").Return(props)
	nmsg.On("GetContentType").Return("json")
	nmsg.On("GetData").Return("hello")
	nmsg.On("Abandon").Return(nil)

	subMock.Receives = append(subMock.Receives, nmsg)
	tmock.On("NewSubscription").Return(subMock, nil)

	mmock.On("Create").Return(nil)
	rules := make([]*servicebus.RuleEntity, 0)
	mmock.On("ListRules").Return(rules, nil)

	amock.On("Connect").Return(nil)
	amock.On("NewTopic", "topicName").Return(tmock, nil)
	amock.On("NewSubscriptionManager").Return(mmock, nil)

	oldNewAzureNS := NewAzureNS

	NewAzureNS = func(string) AzureNamespaceShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAzureNS = oldNewAzureNS
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Address: &pb.Address{Name: "topicName"}}
	mc := make(chan *pb.Message)
	defer close(mc)
	stop := make(chan bool)
	go func() {
		suberr := prov.Subscribe(&ctx, src, mc, stop)
		assert.Nil(t, suberr)
	}()

	msg := <-mc
	time.Sleep(100 * time.Millisecond)
	err = prov.Nack(&ctx, msg.GetUuid())
	assert.NotNil(t, msg)
	assert.Nil(t, err)

	subMock.AssertExpectations(t)
	tmock.AssertExpectations(t)
	mmock.AssertExpectations(t)
	amock.AssertExpectations(t)
}

func Test_Nack_AbandonError(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureNSMock{}
	tmock := &azureTopicMock{}
	mmock := &azureSMMock{}
	subMock := &azureSubMock{}
	tmock.On("GetName").Return("topicName")
	tmock.On("Close").Return(nil)
	subMock.On("Receive", ctx, mock.Anything).Return(nil)
	subMock.On("Close").Return(nil)
	subMock.On("Name").Return("sub")

	subMock.Receives = make([]*azureMsgMock, 0)
	nmsg := &azureMsgMock{}
	props := make(map[string]interface{})
	props["header1"] = "value"
	nmsg.On("SetData").Return(nil)
	nmsg.On("GetUserProperties").Return(props)
	nmsg.On("GetContentType").Return("json")
	nmsg.On("GetData").Return("hello")
	nmsg.On("Abandon").Return(errors.New("AbandonError"))

	subMock.Receives = append(subMock.Receives, nmsg)
	tmock.On("NewSubscription").Return(subMock, nil)

	mmock.On("Create").Return(nil)
	rules := make([]*servicebus.RuleEntity, 0)
	mmock.On("ListRules").Return(rules, nil)

	amock.On("Connect").Return(nil)
	amock.On("NewTopic", "topicName").Return(tmock, nil)
	amock.On("NewSubscriptionManager").Return(mmock, nil)

	oldNewAzureNS := NewAzureNS

	NewAzureNS = func(string) AzureNamespaceShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAzureNS = oldNewAzureNS
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Address: &pb.Address{Name: "topicName"}}
	mc := make(chan *pb.Message)
	defer close(mc)
	stop := make(chan bool)
	go func() {
		suberr := prov.Subscribe(&ctx, src, mc, stop)
		assert.Nil(t, suberr)
	}()

	msg := <-mc
	assert.NotNil(t, msg)
	time.Sleep(100 * time.Millisecond)
	err = prov.Nack(&ctx, msg.GetUuid())
	assert.NotNil(t, err)
	assert.Equal(t, err.GetMessage(), "AbandonError")

	subMock.AssertExpectations(t)
	tmock.AssertExpectations(t)
	mmock.AssertExpectations(t)
	amock.AssertExpectations(t)
}

func Test_Retry(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()

	oldGetClientIdentifier := GetClientIdentifier
	GetClientIdentifier = func(context.Context) (string, error) {
		return "1234", nil
	}

	amock := &azureNSMock{}
	tmock := &azureTopicMock{}
	mmock := &azureSMMock{}
	subMock := &azureSubMock{}
	tmock.On("GetName").Return("topicName")
	tmock.On("Close").Return(nil)
	subMock.On("Receive", ctx, mock.Anything).Return(nil)
	subMock.On("Close").Return(nil)
	subMock.On("Name").Return("sub")

	subMock.Receives = make([]*azureMsgMock, 0)
	nmsg := &azureMsgMock{}
	props := make(map[string]interface{})
	props["header1"] = "value"
	nmsg.On("SetData").Return(nil)
	nmsg.On("GetUserProperties").Return(props)
	nmsg.On("GetContentType").Return("json")
	nmsg.On("GetData").Return("hello")
	nmsg.On("Abandon").Return(nil)
	nmsg.On("GetID").Return("1234")
	nmsg.On("GetDeliveryCount").Return(0)
	nmsg.On("Complete").Return(nil)

	subMock.Receives = append(subMock.Receives, nmsg)
	tmock.On("NewSubscription").Return(subMock, nil)
	tmock.On("ScheduleAt").Return(nil, nil)

	mmock.On("Create").Return(nil)
	rules := make([]*servicebus.RuleEntity, 0)
	mmock.On("ListRules").Return(rules, nil)
	amock.On("Connect").Return(nil)
	amock.On("NewTopic", "topicName").Return(tmock, nil)
	amock.On("NewSubscriptionManager").Return(mmock, nil)

	oldNewAzureNS := NewAzureNS

	NewAzureNS = func(string) AzureNamespaceShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAzureNS = oldNewAzureNS
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)

	src := &pb.Source{Address: &pb.Address{Name: "topicName"}}
	mc := make(chan *pb.Message)
	defer close(mc)
	stop := make(chan bool)
	go func() {
		suberr := prov.Subscribe(&ctx, src, mc, stop)
		assert.Nil(t, suberr)
	}()

	msg := <-mc
	time.Sleep(100 * time.Millisecond)
	err = prov.Retry(&ctx, src, msg.GetUuid(), 1)
	assert.NotNil(t, msg)
	assert.Nil(t, err)

	subMock.AssertExpectations(t)
	tmock.AssertExpectations(t)
	mmock.AssertExpectations(t)
	amock.AssertExpectations(t)
}

func Test_Ack_NoConnect(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()
	msg := pb.Message{}
	err := prov.Ack(&ctx, msg.GetUuid())
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "Could not retrieve client-id from context")
}

func Test_Nack_NoConnect(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()
	msg := pb.Message{}
	err := prov.Nack(&ctx, msg.GetUuid())
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "Could not retrieve client-id from context")
}
func Test_Publish_NoConnect(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()
	mc := make(chan *pb.Message)
	ec := make(chan *pb.Error)
	err := prov.Publish(&ctx, mc, ec)
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "Could not retrieve client-id from context")
}

func Test_Subscribe_NoConnect(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()
	address := &pb.Address{Name: "topicName"}
	src := &pb.Source{Address: address}
	mc := make(chan *pb.Message)

	stop := make(chan bool)

	err := prov.Subscribe(&ctx, src, mc, stop)
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "Could not retrieve client-id from context")
}
func Test_Subscribe_NotopicName(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()
	address := &pb.Address{}
	src := &pb.Source{Address: address}
	mc := make(chan *pb.Message)

	stop := make(chan bool)

	err := prov.Subscribe(&ctx, src, mc, stop)
	assert.NotNil(t, err)
	assert.Contains(t, err.GetMessage(), "address name not defined")
}

func Test_Subscribe_NoAddress(t *testing.T) {
	prov := NewAzureProvider()
	ctx := context.Background()
	src := &pb.Source{}
	mc := make(chan *pb.Message)

	stop := make(chan bool)

	err := prov.Subscribe(&ctx, src, mc, stop)
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

	amock := &azureNSMock{}
	amock.On("Connect").Return(nil)
	oldNewAzureNS := NewAzureNS

	NewAzureNS = func(string) AzureNamespaceShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAzureNS = oldNewAzureNS
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)
	prov.Disconnect(&ctx)

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

	amock := &azureNSMock{blockConnect: 1}
	amock.On("Connect").Return(nil)

	oldNewAzureNS := NewAzureNS

	NewAzureNS = func(string) AzureNamespaceShim {
		return amock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAzureNS = oldNewAzureNS
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)

	assert.Nil(t, err)
	connected := prov.WaitForConnect(&ctx)

	assert.True(t, connected)

	amock.AssertExpectations(t)

}

func Test_Publish(t *testing.T) {
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

	amock := &azureNSMock{}
	tmock := &azureTopicMock{}
	msgMock := &azureMsgMock{}
	tmock.On("GetName").Return("topicName")
	tmock.On("Close").Return(nil)
	tmock.On("Send").Return(nil)

	amock.On("Connect").Return(nil)
	amock.On("NewTopic", "topicName").Return(tmock, nil)
	oldNewAzureNS := NewAzureNS

	NewAzureNS = func(string) AzureNamespaceShim {
		return amock
	}

	oldNewAzureMsg := NewAzureMsg
	NewAzureMsg = func() AzureMessageShim {
		return msgMock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAzureNS = oldNewAzureNS
		NewAzureMsg = oldNewAzureMsg
	}()

	mc := make(chan *pb.Message)
	errchan := make(chan *pb.Error)

	go func() {
		mc <- msg
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)

	go func() {
		suberr := prov.Publish(&ctx, mc, errchan)
		assert.Nil(t, suberr)
	}()

	time.Sleep(100 * time.Millisecond)

	amock.AssertExpectations(t)
}

func Test_Publish_Error(t *testing.T) {
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

	amock := &azureNSMock{}
	tmock := &azureTopicMock{}
	msgMock := &azureMsgMock{}
	tmock.On("GetName").Return("topicName")
	tmock.On("Close").Return(nil)
	tmock.On("Send").Return(errors.New("an error occured"))

	amock.On("Connect").Return(nil)
	amock.On("NewTopic", "topicName").Return(tmock, nil)
	oldNewAzureNS := NewAzureNS

	NewAzureNS = func(string) AzureNamespaceShim {
		return amock
	}

	oldNewAzureMsg := NewAzureMsg
	NewAzureMsg = func() AzureMessageShim {
		return msgMock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAzureNS = oldNewAzureNS
		NewAzureMsg = oldNewAzureMsg
	}()

	mc := make(chan *pb.Message)
	errchan := make(chan *pb.Error)

	go func() {
		mc <- msg
	}()

	ctx := context.Background()
	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)

	go func() {
		suberr := prov.Publish(&ctx, mc, errchan)
		assert.Nil(t, suberr)
	}()

	time.Sleep(100 * time.Millisecond)

	pbErr := <-errchan
	assert.NotNil(t, pbErr)
	assert.Equal(t, pbErr.GetMessage(), "an error occured")

	amock.AssertExpectations(t)
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
		Durable:       true,
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

	amock := &azureNSMock{}
	tmock := &azureTopicMock{}
	msgMock := &azureMsgMock{}
	smMock := &azureSMMock{}
	smMockParent := &azureSMMock{}
	subMock := &azureSubMock{}

	subMock.On("Receive", ctx, mock.Anything).Return(nil)
	subMock.On("Close").Return(nil)
	subMock.On("Name").Return("sub")

	subMock.Receives = make([]*azureMsgMock, 0)
	nmsg := &azureMsgMock{}
	props := make(map[string]interface{})
	props["header1"] = "value"
	nmsg.On("GetUserProperties").Return(props)
	nmsg.On("GetContentType").Return("json")
	nmsg.On("GetData").Return("hello")
	nmsg.On("Complete").Return(nil)

	subMock.Receives = append(subMock.Receives, nmsg)

	tmock.On("GetName").Return("topicName")
	tmock.On("Close").Return(nil)
	tmock.On("GetEntity").Return(&servicebus.TopicEntity{})
	tmock.On("NewSubscription", mock.AnythingOfType("string"), mock.Anything).Return(subMock, nil).Twice()

	smMock.On("Create").Return(nil)
	smMockParent.On("Create").Return(nil)
	rules := make([]*servicebus.RuleEntity, 0)
	smMock.On("ListRules").Return(rules, nil)
	smMockParent.On("ListRules").Return(rules, nil)

	smMock.On("PutRule",
		mock.AnythingOfType("string"),
		"RoutingAndFilterRule",
		"(user.RoutingKey = 'subject1' OR user.RoutingKey = 'subject2')",
	).Return(&servicebus.RuleEntity{}, nil).Once()

	smMockParent.On("PutRule",
		mock.AnythingOfType("string"),
		"RoutingAndFilterRule",
		`(user.RoutingKey = 'subject1' OR user.RoutingKey = 'subject2') AND (("key1" = 'value1' OR "key2" = 'value2') OR ("key3" = 'value3' OR "key4" = 'value4'))`,
	).Return(&servicebus.RuleEntity{}, nil).Once()

	amock.On("NewSubscriptionManager").Return(smMock, nil).Once()
	amock.On("NewSubscriptionManager").Return(smMockParent, nil).Once()
	amock.On("Connect").Return(nil)
	amock.On("NewTopic", "topicName").Return(tmock, nil)
	amock.On("NewTopic", "parent").Return(tmock, nil)

	oldNewAzureNS := NewAzureNS

	NewAzureNS = func(string) AzureNamespaceShim {
		return amock
	}

	oldNewAzureMsg := NewAzureMsg
	NewAzureMsg = func() AzureMessageShim {
		return msgMock
	}

	defer func() {
		GetClientIdentifier = oldGetClientIdentifier
		NewAzureNS = oldNewAzureNS
		NewAzureMsg = oldNewAzureMsg
	}()

	cc := &pb.ConnectionConfiguration{}
	err := prov.Connect(&ctx, cc, false)
	assert.Nil(t, err)
	var msg *pb.Message

	mc := make(chan *pb.Message)
	defer close(mc)

	stop := make(chan bool)

	go func(stop chan bool) {
		suberr := prov.Subscribe(&ctx, src, mc, stop)
		assert.Nil(t, suberr)
	}(stop)

	go func() {
		msg = <-mc
	}()

	time.Sleep(100 * time.Millisecond)
	err = prov.Ack(&ctx, msg.GetUuid())
	assert.NotNil(t, msg)
	assert.Nil(t, err)
	assert.NotNil(t, msg.GetAddress())
	assert.Equal(t, msg.GetAddress(), src.GetAddress())
	assert.Equal(t, msg.GetAddress().GetSubjects(), subjects)

	amock.AssertExpectations(t)
	tmock.AssertExpectations(t)
	smMock.AssertExpectations(t)
	// smMockParent.AssertExpectations(t)
	nmsg.AssertExpectations(t)
	subMock.AssertExpectations(t)
}

func Test_declareExchange_errorAddressType(t *testing.T) {

	// prov := NewAzureProvider()
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

func Test_sourceNameToSubName_ShortName(t *testing.T) {
	srcName := "ShortName"
	subName := sourceNameToSubName(srcName)
	idx := strings.Index(subName, "-")
	assert.Equal(t, idx, 9)
}
