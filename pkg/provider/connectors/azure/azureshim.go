package azure

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-amqp-common-go/v3/uuid"
	servicebus "github.com/Azure/azure-service-bus-go"
	"sassoftware.io/convoy/arke/pkg/util"
)

// AzureNamespaceShim interface for namespace
type AzureNamespaceShim interface {
	Connect() error
	NewSubscriptionManager(string) (AzureSubscriptionManagerShim, error)
	NewTopic(*context.Context, string) (AzureTopicShim, error)
}

// AzureTopicShim interface for topic
type AzureTopicShim interface {
	Close(*context.Context) error
	GetEntity() *servicebus.TopicEntity
	GetName() string
	NewSubscription(string, ...servicebus.SubscriptionOption) (AzureSubscriptionShim, error)
	ScheduleAt(*context.Context, time.Time, ...AzureMessageShim) ([]int64, error)
	Send(*context.Context, AzureMessageShim, ...servicebus.SendOption) error
}

// AzureSubscriptionManagerShim interface for subscription manager
type AzureSubscriptionManagerShim interface {
	Create(*context.Context, string, ...servicebus.SubscriptionManagementOption) error
	DeleteRule(*context.Context, string, string) error
	ListRules(*context.Context, string) ([]*servicebus.RuleEntity, error)
	PutRule(*context.Context, string, string, string) (*servicebus.RuleEntity, error)
}

// AzureSubscriptionShim interface for subscription
type AzureSubscriptionShim interface {
	Close(*context.Context) error
	Receive(*context.Context, chan AzureMessageShim) error
}

// AzureMessageShim interface for messages
type AzureMessageShim interface {
	Abandon(*context.Context) error
	Complete(*context.Context) error

	GetContentType() string
	GetData() []byte
	GetDeliveryCount() uint32
	GetID() string
	GetUserProperties() map[string]interface{}
	GetUserProperty(string) (interface{}, bool)

	SetContentType(string)
	SetData([]byte)
	SetLockToken(*uuid.UUID)
	SetUserProperties(map[string]interface{})
	SetUserProperty(string, interface{})
}

// AzureMessage message
type AzureMessage struct {
	AzureMessageShim
	sbMsg *servicebus.Message
}

// AzureNamespace namespace
type AzureNamespace struct {
	AzureNamespaceShim
	namespace        *servicebus.Namespace
	topicManager     *servicebus.TopicManager
	connectionString string
}

// AzureTopic topic
type AzureTopic struct {
	AzureTopicShim
	topic       *servicebus.Topic
	topicEntity *servicebus.TopicEntity
}

// AzureSubscription subscription
type AzureSubscription struct {
	subscription *servicebus.Subscription
}

// AzureSubscriptionManager subscription manager
type AzureSubscriptionManager struct {
	AzureSubscriptionManagerShim
	subscriptionManager *servicebus.SubscriptionManager
}

// NewAzureNamespace create a new namespace object
func NewAzureNamespace(connStr string) AzureNamespaceShim {
	return &AzureNamespace{connectionString: connStr}
}

// Connect create a connection to the azure namespace
func (an *AzureNamespace) Connect() error {
	ns, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(an.connectionString))
	if err != nil {
		return err
	}
	an.namespace = ns
	an.topicManager = an.namespace.NewTopicManager()
	return nil
}

// NewTopic create a new topic
func (an *AzureNamespace) NewTopic(ctx *context.Context, topicName string) (AzureTopicShim, error) {

	topicEntity, err := an.topicManager.Get(*ctx, topicName)
	if err != nil {
		return nil, err
	}

	topic, err := an.namespace.NewTopic(topicEntity.Name)
	if err != nil {
		return nil, err
	}
	ast := &AzureTopic{topic: topic, topicEntity: topicEntity}
	return ast, nil
}

// NewSubscription create a new subscription
func (at *AzureTopic) NewSubscription(name string, opts ...servicebus.SubscriptionOption) (AzureSubscriptionShim, error) {

	sub, err := at.topic.NewSubscription(name, opts...)
	if err != nil {
		return nil, err
	}
	as := &AzureSubscription{subscription: sub}
	return as, nil
}

// NewSubscriptionManager create a new subscription manager
func (an *AzureNamespace) NewSubscriptionManager(topicName string) (AzureSubscriptionManagerShim, error) {
	sm, err := an.namespace.NewSubscriptionManager(topicName)
	if err != nil {
		return nil, err
	}
	asm := &AzureSubscriptionManager{subscriptionManager: sm}
	return asm, nil
}

// ScheduleAt schedule a message
func (at *AzureTopic) ScheduleAt(ctx *context.Context, delay time.Time, messages ...AzureMessageShim) ([]int64, error) {
	sbMessages := make([]*servicebus.Message, 0)
	for _, message := range messages {
		sbMessages = append(sbMessages, message.(*AzureMessage).sbMsg)
	}
	seq, err := at.topic.ScheduleAt(*ctx, delay, sbMessages...)
	if err != nil {
		return nil, err
	}
	return seq, nil
}

// Close close the topic connection
func (at *AzureTopic) Close(ctx *context.Context) error {
	return at.topic.Close(*ctx)
}

// GetEntity get the topic entity
func (at *AzureTopic) GetEntity() *servicebus.TopicEntity {
	return at.topicEntity
}

// GetName get the topic name
func (at *AzureTopic) GetName() string {
	return at.topic.Name
}

// Send send a message to a topic
func (at *AzureTopic) Send(ctx *context.Context, message AzureMessageShim, opts ...servicebus.SendOption) error {
	msg := message.(*AzureMessage)
	return at.topic.Send(*ctx, msg.sbMsg, opts...)
}

// Create create a new subscription if it does not exist
func (asm *AzureSubscriptionManager) Create(ctx *context.Context, name string, opts ...servicebus.SubscriptionManagementOption) error {

	_, err := asm.subscriptionManager.Get(*ctx, name)
	if err != nil {
		//sm.Use(servicebus.TraceReqAndResponseMiddleware())
		_, err = asm.subscriptionManager.Put(*ctx, name, opts...)

		if err != nil {
			fmt.Printf("error creating subscription: %s\n", err)
			return err
		}
	}
	return nil
}

// ListRules list filter rules on a subscription
func (asm *AzureSubscriptionManager) ListRules(ctx *context.Context, name string) ([]*servicebus.RuleEntity, error) {
	re, err := asm.subscriptionManager.ListRules(*ctx, name)
	if err != nil {
		return nil, err
	}
	return re, err
}

// DeleteRule delete a rule on a subscription
func (asm *AzureSubscriptionManager) DeleteRule(ctx *context.Context, subscriptionName, ruleName string) error {
	return asm.subscriptionManager.DeleteRule(*ctx, subscriptionName, ruleName)
}

// PutRule create a rule on a subscription
func (asm *AzureSubscriptionManager) PutRule(ctx *context.Context, subscriptionName, ruleName string, ruleText string) (*servicebus.RuleEntity, error) {
	filter := &servicebus.SQLFilter{Expression: ruleText}
	return asm.subscriptionManager.PutRule(*ctx, subscriptionName, ruleName, filter)
}

// Close close the subscription connection
func (as *AzureSubscription) Close(ctx *context.Context) error {
	return as.subscription.Close(*ctx)
}

//Receive receive messages on a subscription
func (as *AzureSubscription) Receive(ctx *context.Context, messages chan AzureMessageShim) error {
	// recover sending on closed channel issues
	defer func() error {
		if err := recover(); err != nil {
			util.Logger.Debugf("recovered: %v", err)
			return fmt.Errorf("%s", err)
		}
		return nil
	}()

	as.subscription.Receive(*ctx, servicebus.HandlerFunc(func(ctx context.Context, msg *servicebus.Message) error {
		amsg := &AzureMessage{sbMsg: msg}
		messages <- amsg
		return nil
	}))
	return nil
}

func (m *AzureMessage) SetData(data []byte) {
	m.sbMsg.Data = data
}

func (m *AzureMessage) SetLockToken(token *uuid.UUID) {
	m.sbMsg.LockToken = token
}

func (m *AzureMessage) UserProperties(properties map[string]interface{}) {
	m.sbMsg.UserProperties = properties
}

func (m *AzureMessage) SetUserProperties(props map[string]interface{}) {
	m.sbMsg.UserProperties = props
}

func (m *AzureMessage) SetUserProperty(key string, value interface{}) {
	m.sbMsg.UserProperties[key] = value
}

func (m *AzureMessage) GetUserProperty(key string) (interface{}, bool) {
	val, ok := m.sbMsg.UserProperties[key]
	return val, ok
}

func (m *AzureMessage) GetUserProperties() map[string]interface{} {
	return m.sbMsg.UserProperties
}

func (m *AzureMessage) GetID() string {
	return m.sbMsg.ID
}

func (m *AzureMessage) GetData() []byte {
	return m.sbMsg.Data
}

func (m *AzureMessage) GetDeliveryCount() uint32 {
	return m.sbMsg.DeliveryCount
}

func (m *AzureMessage) GetContentType() string {
	return m.sbMsg.ContentType
}

func (m *AzureMessage) SetContentType(contentType string) {
	m.sbMsg.ContentType = contentType
}

func (m *AzureMessage) Abandon(ctx *context.Context) error {
	return m.sbMsg.Abandon(*ctx)
}

func (m *AzureMessage) Complete(ctx *context.Context) error {
	return m.sbMsg.Complete(*ctx)
}

func NewAzureMessage() AzureMessageShim {
	msg := &AzureMessage{}
	msg.sbMsg = &servicebus.Message{}
	return msg
}
